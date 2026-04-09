# ==========================================
# Controle de Estoque — JIOR BLANC
# Streamlit + SQLite Persistente
# ==========================================
import os
import re
import io
import shutil
import sqlite3
import datetime
from datetime import datetime as dt
from typing import Optional, Tuple, List, Dict
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# ==========================================
# Config & Database - SQLITE PERSISTENTE
# ==========================================
st.set_page_config(page_title="Estoque JIOR BLANC", page_icon="📦", layout="wide")

# Usar diretório persistente do Streamlit Cloud
if 'STREAMLIT_CLOUD' in os.environ:
    BASE_DIR = "/mount/src/estoque-bb"
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "estoque.db")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")

# Garante que o diretório existe
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def backup_database():
    """Faz backup do banco de dados"""
    timestamp = dt.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(BACKUP_DIR, f"estoque_backup_{timestamp}.db")
    if os.path.exists(DB_PATH):
        shutil.copy2(DB_PATH, backup_path)
    else:
        open(backup_path, "wb").close()
    return backup_path

@st.cache_resource(show_spinner=False)
def init_db() -> None:
    con = get_conn()
    cur = con.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            category TEXT NOT NULL,
            subtype TEXT NOT NULL,
            sku_base TEXT,
            custo_unitario REAL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS variants (
            id INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            color TEXT NOT NULL,
            size TEXT NOT NULL,
            sku TEXT NOT NULL UNIQUE,
            custo_unitario REAL
        );
        CREATE TABLE IF NOT EXISTS movements (
            id INTEGER PRIMARY KEY,
            variant_id INTEGER NOT NULL REFERENCES variants(id) ON DELETE CASCADE,
            qty INTEGER NOT NULL,
            reason TEXT NOT NULL,
            ts TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sku_mapping (
            id INTEGER PRIMARY KEY,
            sku_pdf TEXT NOT NULL UNIQUE,
            sku_estoque TEXT NOT NULL REFERENCES variants(sku) ON DELETE CASCADE
        );
        CREATE VIEW IF NOT EXISTS stock_view AS
        SELECT v.id AS variant_id, v.sku, COALESCE(SUM(m.qty),0) AS stock
        FROM variants v
        LEFT JOIN movements m ON m.variant_id = v.id
        GROUP BY v.id, v.sku;
        CREATE VIEW IF NOT EXISTS stock_value_view AS
        SELECT v.sku, p.category, p.subtype, v.color, v.size,
               COALESCE(SUM(m.qty),0) AS estoque,
               COALESCE(v.custo_unitario, p.custo_unitario, 0) AS custo_unitario,
               (COALESCE(SUM(m.qty),0) * COALESCE(v.custo_unitario, p.custo_unitario, 0)) AS valor_estoque
        FROM variants v
        JOIN products p ON p.id = v.product_id
        LEFT JOIN movements m ON m.variant_id = v.id
        GROUP BY v.id, v.sku;
    """)
    con.commit()

def migrate_db() -> None:
    """Migra o banco de dados para a versão mais recente"""
    con = get_conn()
    cur = con.cursor()
    
    # sku_base em products
    try:
        cur.execute("SELECT sku_base FROM products LIMIT 1")
    except sqlite3.OperationalError:
        cur.execute("ALTER TABLE products ADD COLUMN sku_base TEXT")
        st.info("✓ Coluna sku_base adicionada à tabela products")
    
    # custo_unitario em products
    try:
        cur.execute("SELECT custo_unitario FROM products LIMIT 1")
    except sqlite3.OperationalError:
        cur.execute("ALTER TABLE products ADD COLUMN custo_unitario REAL DEFAULT 0")
        st.info("✓ Coluna custo_unitario adicionada à tabela products")
    
    # custo_unitario em variants
    try:
        cur.execute("SELECT custo_unitario FROM variants LIMIT 1")
    except sqlite3.OperationalError:
        cur.execute("ALTER TABLE variants ADD COLUMN custo_unitario REAL")
        st.info("✓ Coluna custo_unitario adicionada à tabela variants")
    
    # tabela sku_mapping
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sku_mapping'")
    if not cur.fetchone():
        cur.execute("""
            CREATE TABLE sku_mapping (
                id INTEGER PRIMARY KEY,
                sku_pdf TEXT NOT NULL UNIQUE,
                sku_estoque TEXT NOT NULL REFERENCES variants(sku) ON DELETE CASCADE
            )
        """)
        st.info("✓ Tabela sku_mapping criada")
    
    con.commit()

# ==========================================
# Helpers: SKU
# ==========================================
def generate_sku(sku_base: str, color: str, size: str) -> str:
    cor_limpa = re.sub(r'[^a-zA-Z0-9ÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇáàâãéèêíìîóòôõúùûç ]', '', color.strip()).strip().title().replace(" ", "")
    tamanho_limpo = re.sub(r'[^A-Za-z0-9]', '', size.strip().upper())
    sku_base_limpo = sku_base.strip().upper().replace(" ", "")
    return f"{sku_base_limpo}-{cor_limpa}-{tamanho_limpo}"

def sanitize_sku(s: str) -> str:
    s = (s or "").strip().upper().replace(" ", "")
    return re.sub(r"[^A-Z0-9\-_ÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ]", "", s)

def normalize_key(s: str) -> str:
    return re.sub(r'[^A-Z0-9]', '', sanitize_sku(s))

def sanitized_to_original_sku_map() -> Dict[str, str]:
    vdf = list_variants_df()
    orig_list = vdf["sku"].astype(str).tolist()
    return {sanitize_sku(s): s for s in orig_list}

# ==========================================
# CRUD Operations
# ==========================================
def get_or_create_product(category: str, subtype: str, sku_base: Optional[str] = None, custo_unitario: Optional[float] = None) -> int:
    """
    Regra:
    - Se o produto NÃO existir: cria e pode receber custo_unitario (se fornecido).
    - Se o produto JÁ existir:
        - Atualiza sku_base SE sku_base for passado (não None).
        - Atualiza custo_unitario SÓ se custo_unitario for passado (não None).
        - Se custo_unitario for None, NÃO mexe no custo já cadastrado.
    """
    con = get_conn()
    cur = con.cursor()
    cur.execute(
        "SELECT id FROM products WHERE category=? AND subtype=?",
        (category.strip(), subtype.strip())
    )
    row = cur.fetchone()

    # Já existe
    if row:
        updates = []
        params = []

        # Atualiza SKU base se foi informado algo (pode ser string vazia para limpar)
        if sku_base is not None:
            updates.append("sku_base = ?")
            params.append(sku_base.strip() if sku_base else None)

        # Só atualiza custo se foi passado explicitamente
        if custo_unitario is not None:
            updates.append("custo_unitario = ?")
            params.append(float(custo_unitario))

        if updates:
            params.append(row[0])
            cur.execute(
                f"UPDATE products SET {', '.join(updates)} WHERE id = ?",
                params
            )
            con.commit()

        return row[0]

    # Não existe: criar
    try:
        cur.execute(
            "INSERT INTO products(category, subtype, sku_base, custo_unitario) VALUES(?,?,?,?)",
            (
                category.strip(),
                subtype.strip(),
                sku_base.strip() if sku_base else None,
                float(custo_unitario) if custo_unitario is not None else 0.0
            )
        )
    except sqlite3.OperationalError:
        # Banco antigo sem coluna de custo / sku_base
        cur.execute(
            "INSERT INTO products(category, subtype) VALUES(?,?)",
            (category.strip(), subtype.strip())
        )

    con.commit()
    return cur.lastrowid


def create_variant(category: str, subtype: str, color: str, size: str, sku_base: Optional[str] = None, sku_override: Optional[str] = None, custo_unitario_produto: float = 0, custo_unitario_variante: Optional[float] = None) -> Tuple[bool, str]:
    con = get_conn()
    cur = con.cursor()
    product_id = get_or_create_product(category, subtype, sku_base)
    
    if not sku_base:
        try:
            cur.execute("SELECT sku_base FROM products WHERE id=?", (product_id,))
            sku_base_row = cur.fetchone()
            sku_base = sku_base_row[0] if sku_base_row and sku_base_row[0] else None
        except sqlite3.OperationalError:
            sku_base = None
    
    if sku_base:
        sku_auto = generate_sku(sku_base, color, size)
    else:
        def part(x: str, n: int) -> str:
            return x.strip()[:n].upper() if x else "X"
        sku_auto = f"{part(category,4)}-{part(subtype,4)}-{part(color,3)}-{part(size,4)}"
    
    sku = sanitize_sku(sku_override or sku_auto)
    try:
        cur.execute("INSERT INTO variants(product_id, color, size, sku, custo_unitario) VALUES(?,?,?,?,?)",
                   (product_id, color.strip(), size.strip(), sku, float(custo_unitario_variante) if custo_unitario_variante else None))
        con.commit()
        return True, sku
    except sqlite3.IntegrityError as e:
        return False, f"Não foi possível criar a variante. SKU já existe? Detalhe: {e}"

def record_movement(sku: str, qty: int, reason: str) -> None:
    con = get_conn()
    cur = con.cursor()
    cur.execute("SELECT id FROM variants WHERE sku=?", (sku,))
    row = cur.fetchone()
    if not row:
        raise ValueError("SKU não encontrado.")
    variant_id = row[0]
    cur.execute("INSERT INTO movements(variant_id, qty, reason, ts) VALUES(?,?,?,?)",
               (variant_id, qty, reason, datetime.datetime.now().isoformat(timespec="seconds")))
    con.commit()

def update_variant(old_sku: str, new_sku: str, category: str, subtype: str, color: str, size: str, sku_base: Optional[str] = None, custo_unitario_produto: Optional[float] = None, custo_unitario_variante: Optional[float] = None) -> Tuple[bool, str]:
    try:
        backup_database()
        con = get_conn()
        cur = con.cursor()
        cur.execute("SELECT id, product_id FROM variants WHERE sku=?", (old_sku,))
        variant = cur.fetchone()
        if not variant:
            return False, "SKU não encontrado."
        variant_id, old_product_id = variant
        
        if old_sku != new_sku:
            cur.execute("SELECT id FROM variants WHERE sku=?", (new_sku,))
            if cur.fetchone():
                return False, "Novo SKU já existe no sistema."
        
        new_product_id = get_or_create_product(category, subtype, sku_base)
        cur.execute("UPDATE variants SET sku=?, color=?, size=?, product_id=?, custo_unitario=? WHERE id=?",
                   (new_sku, color.strip(), size.strip(), new_product_id, float(custo_unitario_variante) if custo_unitario_variante is not None else None, variant_id))
        
        cur.execute("SELECT COUNT(*) FROM variants WHERE product_id=?", (old_product_id,))
        if cur.fetchone()[0] == 0:
            cur.execute("DELETE FROM products WHERE id=?", (old_product_id,))
        con.commit()
        return True, "Variante atualizada com sucesso!"
    except sqlite3.Error as e:
        return False, f"Erro ao atualizar variante: {e}"

def update_sku_base_bulk(category: str, subtype: str, new_sku_base: str) -> Tuple[bool, str]:
    try:
        backup_database()
        con = get_conn()
        cur = con.cursor()
        cur.execute("SELECT id FROM products WHERE category=? AND subtype=?", (category, subtype))
        product = cur.fetchone()
        if not product:
            return False, "Produto não encontrado."
        product_id = product[0]
        cur.execute("SELECT id, color, size FROM variants WHERE product_id=?", (product_id,))
        variants = cur.fetchall()
        for variant_id, color, size in variants:
            new_sku = generate_sku(new_sku_base, color, size)
            cur.execute("UPDATE variants SET sku=? WHERE id=?", (new_sku, variant_id))
        try:
            cur.execute("UPDATE products SET sku_base=? WHERE id=?", (new_sku_base, product_id))
        except sqlite3.OperationalError:
            return False, "A coluna sku_base não existe. Execute a migração do banco de dados primeiro."
        con.commit()
        return True, f"SKU base atualizado e {len(variants)} variantes regeneradas com sucesso!"
    except sqlite3.Error as e:
        return False, f"Erro ao atualizar SKU base: {e}"

def update_custo_unitario(category: str, subtype: str, novo_custo: float) -> Tuple[bool, str]:
    try:
        backup_database()
        con = get_conn()
        cur = con.cursor()
        cur.execute("SELECT id FROM products WHERE category=? AND subtype=?", (category, subtype))
        product = cur.fetchone()
        if not product:
            return False, "Produto não encontrado."
        product_id = product[0]
        cur.execute("UPDATE products SET custo_unitario=? WHERE id=?", (novo_custo, product_id))
        con.commit()
        return True, f"Custo unitário (PRODUTO) atualizado para R$ {novo_custo:.2f}"
    except sqlite3.Error as e:
        return False, f"Erro ao atualizar custo unitário: {e}"

def delete_variant(sku: str) -> Tuple[bool, str]:
    try:
        backup_database()
        con = get_conn()
        cur = con.cursor()
        cur.execute("SELECT id, product_id FROM variants WHERE sku=?", (sku,))
        variant = cur.fetchone()
        if not variant:
            return False, "SKU não encontrado."
        variant_id, product_id = variant
        try:
            cur.execute("DELETE FROM sku_mapping WHERE sku_estoque=?", (sku,))
        except sqlite3.OperationalError:
            pass
        cur.execute("DELETE FROM variants WHERE id=?", (variant_id,))
        cur.execute("SELECT COUNT(*) FROM variants WHERE product_id=?", (product_id,))
        if cur.fetchone()[0] == 0:
            cur.execute("DELETE FROM products WHERE id=?", (product_id,))
        con.commit()
        return True, "Variante removida com sucesso!"
    except sqlite3.Error as e:
        return False, f"Erro ao remover variante: {e}"

def get_variant_details(sku: str) -> Optional[dict]:
    con = get_conn()
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT v.sku, p.category, p.subtype, v.color, v.size, v.id, p.id, p.sku_base, p.custo_unitario, v.custo_unitario
            FROM variants v JOIN products p ON p.id = v.product_id WHERE v.sku = ?
        """, (sku,))
    except sqlite3.OperationalError:
        return None
    row = cur.fetchone()
    if row:
        return {
            'sku': row[0], 'category': row[1], 'subtype': row[2], 'color': row[3], 'size': row[4],
            'variant_id': row[5], 'product_id': row[6], 'sku_base': row[7],
            'custo_unitario_produto': row[8] if row[8] is not None else 0,
            'custo_unitario_variante': row[9]
        }
    return None

# ==========================================
# Query Functions
# ==========================================
def list_products_df() -> pd.DataFrame:
    con = get_conn()
    try:
        df = pd.read_sql_query("SELECT id, category, subtype, sku_base, custo_unitario FROM products ORDER BY category, subtype", con)
    except sqlite3.OperationalError:
        df = pd.read_sql_query("SELECT id, category, subtype FROM products ORDER BY category, subtype", con)
        df['sku_base'] = None
        df['custo_unitario'] = 0
    return df

def list_variants_df() -> pd.DataFrame:
    con = get_conn()
    try:
        q = """
            SELECT v.id, v.sku, p.category, p.subtype, v.color, v.size, p.sku_base, 
                   p.custo_unitario AS custo_unitario_produto, v.custo_unitario AS custo_unitario_variante, p.id as product_id
            FROM variants v JOIN products p ON p.id=v.product_id 
            ORDER BY p.category, p.subtype, v.color, v.size
        """
        return pd.read_sql_query(q, con)
    except sqlite3.OperationalError:
        q = """
            SELECT v.id, v.sku, p.category, p.subtype, v.color, v.size, p.id as product_id
            FROM variants v JOIN products p ON p.id=v.product_id 
            ORDER BY p.category, p.subtype, v.color, v.size
        """
        df = pd.read_sql_query(q, con)
        df['sku_base'] = None
        df['custo_unitario_produto'] = 0
        df['custo_unitario_variante'] = None
    return df

def stock_df(filter_text: Optional[str] = None, critical_only: bool = False, critical_value: int = 0) -> pd.DataFrame:
    con = get_conn()
    base_sql = """
        SELECT v.sku, p.category AS categoria, p.subtype AS subtipo, v.color AS cor, v.size AS tamanho,
               COALESCE(s.stock,0) AS estoque, COALESCE(v.custo_unitario, p.custo_unitario, 0) AS custo_unitario,
               (COALESCE(s.stock,0) * COALESCE(v.custo_unitario, p.custo_unitario, 0)) AS valor_estoque
        FROM variants v JOIN products p ON p.id=v.product_id LEFT JOIN stock_view s ON s.variant_id=v.id
    """
    conditions = []
    params = []
    if filter_text:
        conditions.append("v.sku LIKE ? OR p.category LIKE ? OR p.subtype LIKE ? OR v.color LIKE ? OR v.size LIKE ?")
        like = f"%{filter_text}%"
        params = [like, like, like, like, like]
    if critical_only and critical_value > 0:
        conditions.append("COALESCE(s.stock,0) <= ?")
        params.append(critical_value)
    if conditions:
        base_sql += " WHERE " + " AND ".join(conditions)
    base_sql += " ORDER BY p.category, p.subtype, v.color, v.size"
    return pd.read_sql_query(base_sql, get_conn(), params=params)

def stock_value_df(filter_text: Optional[str] = None) -> pd.DataFrame:
    con = get_conn()
    base_sql = "SELECT sku, category, subtype, color, size, estoque, custo_unitario, valor_estoque FROM stock_value_view"
    params = []
    if filter_text:
        base_sql += " WHERE category LIKE ? OR subtype LIKE ? OR color LIKE ? OR size LIKE ?"
        like = f"%{filter_text}%"
        params = [like, like, like, like]
    base_sql += " ORDER BY category, subtype, color, size"
    return pd.read_sql_query(base_sql, con, params=params)

def stock_value_positive_df(filter_text: Optional[str] = None) -> pd.DataFrame:
    """Retorna apenas itens com estoque positivo para cálculo de valor total"""
    con = get_conn()
    base_sql = "SELECT sku, category, subtype, color, size, estoque, custo_unitario, valor_estoque FROM stock_value_view WHERE estoque > 0"
    params = []
    if filter_text:
        base_sql += " AND (category LIKE ? OR subtype LIKE ? OR color LIKE ? OR size LIKE ?)"
        like = f"%{filter_text}%"
        params = [like, like, like, like]
    base_sql += " ORDER BY category, subtype, color, size"
    return pd.read_sql_query(base_sql, con, params=params)

def movements_df(sku_filter: Optional[str] = None, reason: Optional[str] = None, days: Optional[int] = None) -> pd.DataFrame:
    con = get_conn()
    sql = """
        SELECT m.id, v.sku, p.category AS categoria, p.subtype AS subtipo, v.color AS cor, v.size AS tamanho,
               m.qty AS quantidade, m.reason AS motivo, m.ts AS quando
        FROM movements m JOIN variants v ON v.id = m.variant_id JOIN products p ON p.id = v.product_id
    """
    conds, params = [], []
    if sku_filter:
        conds.append("v.sku = ?")
        params.append(sku_filter)
    if reason and reason != "Todos":
        conds.append("m.reason = ?")
        params.append(reason)
    if days:
        ts_min = (datetime.datetime.now() - datetime.timedelta(days=days)).isoformat(timespec="seconds")
        conds.append("m.ts >= ?")
        params.append(ts_min)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY m.ts DESC"
    return pd.read_sql_query(sql, con, params=params)

def get_sales_data(days: Optional[int] = None) -> pd.DataFrame:
    con = get_conn()
    sql = """
        SELECT p.category, p.subtype, v.color, v.size, ABS(SUM(m.qty)) as quantidade_vendida,
               COUNT(*) as numero_vendas, COALESCE(v.custo_unitario, p.custo_unitario, 0) as custo_unitario,
               (ABS(SUM(m.qty)) * COALESCE(v.custo_unitario, p.custo_unitario, 0)) as valor_total_vendido
        FROM movements m JOIN variants v ON v.id = m.variant_id JOIN products p ON p.id = v.product_id
        WHERE m.reason IN ('venda', 'venda_pdf')
    """
    params = []
    if days:
        ts_min = (datetime.datetime.now() - datetime.timedelta(days=days)).isoformat(timespec="seconds")
        sql += " AND m.ts >= ?"
        params.append(ts_min)
    sql += " GROUP BY p.category, p.subtype, v.color, v.size ORDER BY quantidade_vendida DESC"
    return pd.read_sql_query(sql, con, params=params)

# ==========================================
# SKU Mapping helpers
# ==========================================
def get_sku_mapping(sku_pdf_norm: str) -> Optional[str]:
    key_pdf = sanitize_sku(sku_pdf_norm)
    con = get_conn()
    cur = con.cursor()
    try:
        rows = cur.execute("SELECT sku_pdf, sku_estoque FROM sku_mapping").fetchall()
        map1 = {sanitize_sku(k): v for (k, v) in rows}
        if key_pdf in map1:
            return map1[key_pdf]
    except sqlite3.OperationalError:
        pass
    vdf = list_variants_df()
    norm_index = {normalize_key(sku): sku for sku in vdf["sku"].tolist()}
    cand = norm_index.get(normalize_key(key_pdf))
    return cand

# ==========================================
# PDF Parser
# ==========================================
def processar_pdf_vendas(pdf_file) -> Tuple[bool, List[dict], str]:
    try:
        # Tenta importar pypdf primeiro
        try:
            import pypdf as PyPDF2
        except ImportError:
            import PyPDF2
            
        reader = PyPDF2.PdfReader(pdf_file)
        raw = ""
        for p in reader.pages:
            raw += (p.extract_text() or "") + "\n"
        
        # Resto do código do parser PDF permanece EXATAMENTE igual
        lines = [ln.strip() for ln in raw.replace("\r", "\n").split("\n")]
        lines = [ln for ln in lines if ln]
        lines = [re.sub(r"\s+\d+/\d+\s*$", "", ln) for ln in lines]
        
        skip_re = re.compile("|".join([
            r"^LISTA DE RESUMO", r"^\(PRODUTOS DO ARMAZ[EÉ]M\)", r"^PRODUTOS DO ARMAZ[EÉ]M",
            r"^VARIA[CÇ][AÃ]O$", r"^SKU DE PRODUTO$", r"^QTD\.?$", r"^IMPRIMIR.*UPSELLER",
            r"^HTTPS?://", r"^\d+/\d+$", r"^\d{1,2}/\d{1,2}/\d{4}", r"^QTD\. DE PEDIDOS",
            r"^N[ÚU]MERO DE SKUS DE PRODUTOS", r"^TOTAL DE PRODUTOS",
        ]), re.IGNORECASE)
        
        kept = [ln for ln in lines if not skip_re.search(ln)]
        merged = []
        i = 0
        while i < len(kept):
            cur = kept[i]
            if cur.endswith("-") and i + 1 < len(kept):
                cur = cur + kept[i + 1]
                i += 2
                while cur.endswith("-") and i < len(kept):
                    cur = cur + kept[i]
                    i += 1
                merged.append(cur)
            else:
                merged.append(cur)
                i += 1
        
        SIZE = r"(?:XGG|GG|XG|PP|G|M|P|\d{1,3})"
        TOKEN = (
            r"(?:[A-Z]{2,}(?:-[A-Z]{2,}){0,2})"
            r"-(?:[A-Z0-9ÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ]+)"
            r"(?:-[A-Z0-9ÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ]+)?"
            r"-" + SIZE
        )
        sku_pattern = re.compile(rf"({TOKEN})(\d{{1,3}})?", re.UNICODE)
        preface_size_start = re.compile(rf"^(?:{SIZE})(?=(?:[A-Z]{{2,}}(?:-[A-Z]{{2,}}){{0,2}})-)")
        preface_size_after_comma = re.compile(rf",(?:{SIZE})(?=(?:[A-Z]{{2,}}(?:-[A-Z]{{2,}}){{0,2}})-)")
        size_suffix_re = re.compile(rf"^(.*-)(XGG|GG|XG|PP|G|M|P|\d{{1,3}})$")
        recognized_sizes = {'2','4','6','8','10','12','14','16','P','M','G','GG','PP','XG','XGG'}
        
        def norm(s: str) -> str:
            s = s.upper()
            s = re.sub(r"\s+", "", s)
            s = re.sub(r"-{2,}", "-", s)
            return s
        
        def maybe_int(txt: str, next_char: Optional[str] = None) -> Optional[int]:
            if txt is None:
                return None
            if not re.fullmatch(r"\d{1,3}", txt):
                return None
            if next_char == "/":
                return int(txt[0])
            return int(txt)
        
        movimentos: List[dict] = []
        vistos: set = set()
        pending_sku: Optional[str] = None
        
        for ln in merged:
            compact = norm(ln)
            compact = preface_size_start.sub("", compact)
            compact = preface_size_after_comma.sub(",", compact)
            last_end = 0
            
            for m in sku_pattern.finditer(compact):
                token = m.group(1)
                qty_str = m.group(2)
                token_out = token
                ms = size_suffix_re.match(token)
                if ms:
                    size_part = ms.group(2)
                    if qty_str is None and re.fullmatch(r"\d{2,3}", size_part) and size_part not in recognized_sizes:
                        take = ""
                        s = size_part
                        while len(s) > 1 and s not in recognized_sizes:
                            take = s[-1] + take
                            s = s[:-1]
                        if take:
                            qty_str = take
                            token_out = ms.group(1) + s
                    if qty_str is None and re.fullmatch(r"\d{3}", size_part):
                        token_out = ms.group(1) + size_part[:2]
                        qty_str = size_part[2:]
                
                next_char = compact[m.end(2)] if (m.end(2) < len(compact) if qty_str else False) else (compact[m.end(1)] if (m.end(1) < len(compact)) else None)
                qty_val = maybe_int(qty_str, next_char) if qty_str else None
                
                if qty_val is not None:
                    sku_n = norm(token_out)
                    key = (sku_n, qty_val)
                    if key not in vistos:
                        vistos.add(key)
                        mapped = get_sku_mapping(sku_n)
                        movimentos.append({
                            "sku_pdf": sku_n,
                            "sku": mapped or sku_n,
                            "quantidade": int(qty_val),
                            "produto": "Extraído do PDF",
                            "variacao": "Extraído do PDF",
                            "mapeado": bool(mapped),
                        })
                    pending_sku = None
                else:
                    pending_sku = token_out
                last_end = m.end()
            
            if pending_sku:
                tail = compact[last_end:]
                if re.fullmatch(r"\d{1,3}", tail or ""):
                    q = maybe_int(tail, None)
                    if q is not None:
                        sku_n = norm(pending_sku)
                        key = (sku_n, q)
                        if key not in vistos:
                            vistos.add(key)
                            mapped = get_sku_mapping(sku_n)
                            movimentos.append({
                                "sku_pdf": sku_n,
                                "sku": mapped or sku_n,
                                "quantidade": int(q),
                                "produto": "Extraído do PDF",
                                "variacao": "Extraído do PDF",
                                "mapeado": bool(mapped),
                            })
                        pending_sku = None
                else:
                    m2 = re.fullmatch(rf"(?:{SIZE})?(\d{{1,3}})", tail or "")
                    if m2:
                        q = int(m2.group(1))
                        sku_n = norm(pending_sku)
                        key = (sku_n, q)
                        if key not in vistos:
                            vistos.add(key)
                            mapped = get_sku_mapping(sku_n)
                            movimentos.append({
                                "sku_pdf": sku_n,
                                "sku": mapped or sku_n,
                                "quantidade": int(q),
                                "produto": "Extraído do PDF",
                                "variacao": "Extraído do PDF",
                                "mapeado": bool(mapped),
                            })
                        pending_sku = None
        
        if not movimentos:
            return False, [], "Nenhum item encontrado no PDF."
        
        return True, movimentos, f"Encontrados {len(movimentos)} itens no PDF"
    
    except Exception as e:
        import traceback
        st.error(f"Erro detalhado: {traceback.format_exc()}")
        return False, [], f"Erro ao processar PDF: {str(e)}"

# ==========================================
# UI START - CÓDIGO COMPLETO
# ==========================================
init_db()
migrate_db()

st.title("📦 Controle de Estoque — JIOR BLANC")
st.caption("Cadastre produtos, variantes e registre entradas/saídas com histórico e exportação de CSV.")

# Status do banco
st.sidebar.success("✅ SQLite Persistente - Dados Salvos")

# ------------- Sidebar -------------
with st.sidebar:
    st.header("Navegação")
    page = st.radio(
        "Ir para:",
        [
            "Cadastrar Tipo/Subtipo",
            "Cadastrar Variante", 
            "Movimentar Estoque",
            "Baixa por PDF",
            "Estoque Atual",
            "Histórico",
            "Exportar CSV",
            "Editar Variante",
            "Remover Variante",
            "Mapeamento de SKUs",
            "Gerenciar SKU Base",
            "Custo por Categoria/Subtipo (em massa)",
            "Contagem de Estoque",
            "Valor do Estoque",
            "Gráfico de Vendas",
        ],
        index=3,
    )
    
    st.divider()
    st.markdown("**Dica:** nos selects, digite para filtrar o SKU (autocomplete).")
    
    if st.button("🔄 Forçar Migração do Banco"):
        migrate_db()
        st.success("Migração executada com sucesso!")
        st.rerun()
    
    if st.button("💾 Criar Backup Agora"):
        backup_path = backup_database()
        st.success(f"Backup criado: {os.path.basename(backup_path)}")

# ==========================================
# PÁGINAS COMPLETAS
# ==========================================

# -------- Cadastrar Tipo/Subtipo --------
if page == "Cadastrar Tipo/Subtipo":
    st.subheader("Cadastrar novo tipo de produto")
    col1, col2, col3, col4 = st.columns([2, 2, 2, 2])
    with col1:
        category = st.text_input("Categoria (ex.: short, camiseta, moletom)")
    with col2:
        subtype = st.text_input("Subtipo (ex.: tactel, dryfit, algodão, canguru, careca)")
    with col3:
        sku_base = st.text_input("SKU Base (ex.: MOL-CARECA)", help="Usado para gerar SKUs automaticamente: SKUBASE-Cor-Tamanho")
    with col4:
        custo_unitario = st.number_input("Custo Unitário (PRODUTO) R$", min_value=0.0, value=0.0, step=0.01, help="Custo padrão para este tipo/subtipo")
    
    if st.button("Salvar tipo/subtipo", type="primary"):
        if not category or not subtype:
            st.error("Preencha categoria e subtipo.")
        else:
            _ = get_or_create_product(category, subtype, sku_base, custo_unitario)
            if sku_base:
                st.success(f"Tipo/Subtipo salvo: {category} / {subtype} com SKU Base: {sku_base} e custo padrão: R$ {custo_unitario:.2f}")
            else:
                st.success(f"Tipo/Subtipo salvo: {category} / {subtype} com custo padrão: R$ {custo_unitario:.2f}")
    
    st.divider()
    st.subheader("Produtos cadastrados")
    st.dataframe(list_products_df(), use_container_width=True)

# -------- Cadastrar Variante --------
elif page == "Cadastrar Variante":
    st.subheader("Cadastrar nova variante")
    col1, col2, col3, col4, col5 = st.columns([2,2,2,2,2])
    with col1:
        category = st.text_input("Categoria")
    with col2:
        subtype = st.text_input("Subtipo")
    with col3:
        color = st.text_input("Cor")
    with col4:
        size = st.text_input("Tamanho")
    with col5:
        sku_base = st.text_input("SKU Base (opcional — se vazio, usa SKU Base do produto)")
    
    custo_unitario_produto = st.number_input("Custo Unitário (PRODUTO) R$", min_value=0.0, value=0.0, step=0.01, help="Define/atualiza o custo padrão do produto (categoria/subtipo)")
    custo_unitario_variante = st.number_input("Custo Unitário (VARIANTE) R$ (opcional)", min_value=0.0, value=0.0, step=0.01, help="Se informado > 0, esta variante usará este custo (não afeta as outras)")
    sku_override = st.text_input("SKU (opcional — para sobrepor)")
    
    if st.button("Criar variante", type="primary"):
        cvar = custo_unitario_variante if custo_unitario_variante > 0 else None
        ok, msg = create_variant(category, subtype, color, size, sku_base, sku_override, custo_unitario_produto, cvar)
        if ok:
            st.success(f"Variante criada! SKU: {msg}")
        else:
            st.error(msg)

# -------- Movimentar Estoque (saldo antes/depois) --------
elif page == "Movimentar Estoque":
    st.subheader("Movimentar Estoque")
    vdf = list_variants_df()
    sku_options = vdf["sku"].tolist()
    sku = st.selectbox("SKU (digite para filtrar)", sku_options, index=None, placeholder="Digite parte do SKU…")
    
    estoque_atual = None
    if sku:
        df_sku = stock_df(filter_text=sku)
        try:
            estoque_atual = int(df_sku.loc[df_sku["sku"] == sku, "estoque"].values[0])
        except Exception:
            estoque_atual = 0
        st.metric("Estoque atual", estoque_atual)
    
    qtd_input = st.number_input("Quantidade", value=1, step=1, min_value=1)
    reason = st.selectbox(
        "Motivo",
        ["entrada", "venda", "venda_pdf", "ajuste"],
        index=0,
        help="Entrada = positivo; Vendas = negativo; Ajuste = você escolhe o sinal."
    )
    
    if reason == "ajuste":
        sinal = st.radio("Sinal do ajuste", ["positivo (+)", "negativo (-)"], horizontal=True, index=0)
        qty_final = qtd_input if sinal == "positivo (+)" else -qtd_input
    else:
        qty_final = qtd_input if reason == "entrada" else -qtd_input
    
    # NOVA VERIFICAÇÃO: Calcular se faltará estoque
    faltara = 0
    if qty_final < 0 and estoque_atual is not None:
        quantidade_venda = abs(qty_final)
        if quantidade_venda > estoque_atual:
            faltara = quantidade_venda - estoque_atual
            st.warning(f"⚠ **Atenção:** Esta venda excede o estoque! Faltará: **{faltara} unidade(s)**")
    
    st.caption(f"Quantidade aplicada: **{qty_final}** (motivo: **{reason}**)")
    
    if st.button("Registrar movimentação", type="primary"):
        if not sku:
            st.error("Escolha um SKU.")
        elif qty_final == 0:
            st.error("Quantidade não pode ser zero.")
        else:
            try:
                # NOVA LÓGICA: Se for venda e exceder estoque, ajustar para vender apenas o disponível
                quantidade_a_registrar = qty_final
                if reason in ["venda", "venda_pdf"] and estoque_atual is not None and abs(qty_final) > estoque_atual:
                    quantidade_a_registrar = -estoque_atual  # Vende apenas o que tem
                    st.info(f"**Ajustado:** Vendendo apenas {estoque_atual} unidades (estoque disponível)")
                
                record_movement(sku, int(quantidade_a_registrar), reason)
                novo_df_sku = stock_df(filter_text=sku)
                try:
                    novo_estoque = int(novo_df_sku.loc[novo_df_sku["sku"] == sku, "estoque"].values[0])
                except Exception:
                    novo_estoque = (estoque_atual or 0) + int(quantidade_a_registrar)
                
                if faltara > 0:
                    st.success(
                        f"Movimentação registrada: {sku} => {quantidade_a_registrar} ({reason}). "
                        f"Estoque: {estoque_atual} → **{novo_estoque}**. "
                        f"**Faltou vender: {faltara} unidade(s)**"
                    )
                else:
                    st.success(
                        f"Movimentação registrada: {sku} => {quantidade_a_registrar} ({reason}). "
                        f"Estoque: {estoque_atual} → **{novo_estoque}**."
                    )
            except Exception as e:
                st.error(str(e))

# -------- Baixa por PDF (usa processar_pdf_vendas) --------
# -------- Baixa por PDF (usa processar_pdf_vendas) --------
elif page == "Baixa por PDF":
    st.subheader("Baixa por PDF (layout UpSeller)")
    st.caption("Envie o PDF como o do UpSeller. O sistema identifica SKU e quantidade, mapeia e aplica as baixas.")
    
    up = st.file_uploader("Selecionar PDF", type=["pdf"])
    if up is not None:
        file_bytes = up.read()
        ok, movimentos, msg = processar_pdf_vendas(io.BytesIO(file_bytes))
        
        if not ok or not movimentos:
            st.error("Não foi possível identificar itens no PDF. Verifique o layout/arquivo.")
        else:
            st.success(msg)
            df_pdf = pd.DataFrame(movimentos)
            col_order = ["sku_pdf", "sku", "quantidade", "mapeado", "produto", "variacao"]
            df_pdf = df_pdf[[c for c in col_order if c in df_pdf.columns]]
            
            # Coluna editável para correção (começa igual ao lido)
            df_pdf["quantidade_corrigida"] = df_pdf["quantidade"].astype(int)
            
            st.write("Prévia (ajuste a coluna **Qtd. corrigida** se algum valor veio errado do PDF):")
            edited = st.data_editor(
                df_pdf,
                key="pdf_editor",
                use_container_width=True,
                num_rows="dynamic",
                column_config={
                    "sku_pdf": st.column_config.TextColumn("SKU (PDF)", disabled=True),
                    "quantidade": st.column_config.NumberColumn("Qtd. lida (PDF)", disabled=True),
                    "quantidade_corrigida": st.column_config.NumberColumn(
                        "Qtd. corrigida",
                        min_value=1,
                        max_value=999,
                        step=1,
                        help="Altere aqui se a leitura do PDF veio com um zero a mais, etc."
                    ),
                    "sku": st.column_config.TextColumn("SKU (no estoque)"),
                    "mapeado": st.column_config.CheckboxColumn("Mapeado?", disabled=True),
                    "produto": st.column_config.TextColumn("Produto (PDF)", disabled=True),
                    "variacao": st.column_config.TextColumn("Variação (PDF)", disabled=True),
                }
            )
            
            # Checagem fixa de quantidades altas (> 99)
            HIGH_QTY_THRESHOLD = 99
            
            # --- Conferência + Simulação ---
            st.markdown("### Conferência: Itens do PDF vs Estoque Atual")
            
            # Mapas auxiliares
            sku_san_to_orig = sanitized_to_original_sku_map()
            existentes = set(sku_san_to_orig.keys())
            df_estoque_atual = stock_df()
            map_estoque = {str(row["sku"]): int(row["estoque"]) for _, row in df_estoque_atual.iterrows()}
            
            preview = edited.copy()

            def to_original_if_possible(sku_val: str) -> str:
                s = str(sku_val or "")
                s_san = sanitize_sku(s)
                return sku_san_to_orig.get(s_san, s)
            
            # Criar as colunas do preview na ordem correta
            preview["SKU (PDF)"] = preview.get("sku_pdf", "")
            preview["SKU (no estoque)"] = preview.get("sku", "").map(to_original_if_possible)
            preview["Qtd. (PDF)"] = preview.get("quantidade", 0).astype(int)
            
            # usar sempre a corrigida (fallback para lida)
            qtd_usada = preview.get("quantidade_corrigida")
            if qtd_usada is None:
                qtd_usada = preview["Qtd. (PDF)"]
            preview["Qtd. (usada)"] = pd.to_numeric(qtd_usada, errors="coerce").fillna(0).astype(int).clip(lower=0)
            
            preview["Estoque atual (antes)"] = (
                preview["SKU (no estoque)"].map(lambda s: map_estoque.get(str(s), 0)).fillna(0).astype(int)
            )
            
            # REMOVIDO: preview["Estoque após (simulado)"] = preview["Estoque atual (antes)"] - preview["Qtd. (usada)"]
            
            # Criar coluna "Faltará" - quantas unidades não poderão ser vendidas por falta de estoque
            preview["Faltará"] = (preview["Qtd. (usada)"] - preview["Estoque atual (antes)"]).clip(lower=0)
            
            # Calcular quantas unidades SERÃO efetivamente vendidas (não pode ser negativo)
            preview["Será vendido"] = preview["Qtd. (usada)"] - preview["Faltará"]
            
            # Status textual baseado no que faltará
            def status_row(row):
                faltara = row["Faltará"]
                sera_vendido = row["Será vendido"]
                estoque_atual = row["Estoque atual (antes)"]
                
                if faltara > 0:
                    return f"FALTARÁ VENDER {faltara}"
                elif sera_vendido == estoque_atual:
                    return "ZERA ESTOQUE"
                else:
                    return "OK"
            
            preview["Status"] = preview.apply(status_row, axis=1)
            
            # Flag de quantidades muito altas com base na Qtd. (usada)
            preview["Qtd muito alta?"] = preview["Qtd. (usada)"] > HIGH_QTY_THRESHOLD
            
            cols_preview = [
                "SKU (PDF)", "SKU (no estoque)", "Qtd. (PDF)", "Qtd. (usada)", 
                "Estoque atual (antes)", "Será vendido", "Faltará", "Status", "Qtd muito alta?"
            ]
            preview = preview[cols_preview]
            
            # Destaques visuais
            def hl_simulado(row):
                styles = [""] * len(row)
                try:
                    faltara = row.get("Faltará", 0)
                    sera_vendido = row.get("Será vendido", 0)
                    estoque_atual = row.get("Estoque atual (antes)", 0)
                    qtd_alta = row.get("Qtd muito alta?", False)
                    
                    # Se faltará vender algum item
                    if faltara > 0:
                        styles = ["background-color: #ff9966"] * len(row)  # laranja para itens que faltarão
                    # Se zera o estoque
                    elif sera_vendido == estoque_atual:
                        styles = ["background-color: #fff2cc"] * len(row)  # amarelo para itens que zeram estoque
                    # Quantidade muito alta
                    if qtd_alta:
                        styles = ["background-color: #ffe5b4"] * len(row)  # laranja claro
                except:
                    pass
                return styles
            
            show_only_critical = st.toggle("Mostrar apenas itens que faltarão/zeram estoque", value=False)
            filtered_preview = preview.copy()
            if show_only_critical:
                mask_crit = (filtered_preview["Faltará"] > 0) | (filtered_preview["Será vendido"] == filtered_preview["Estoque atual (antes)"])
                filtered_preview = filtered_preview[mask_crit]
                st.caption(f"Exibindo {len(filtered_preview)} de {len(preview)} itens (apenas críticos).")
            
            st.dataframe(filtered_preview.style.apply(hl_simulado, axis=1), use_container_width=True)
            
            # Bloco de conferência de quantidades altas
            df_high = preview[preview["Qtd muito alta?"]].copy()
            confirm_high_needed = not df_high.empty
            confirm_high = False
            
            if confirm_high_needed:
                st.warning(
                    f"⚠️ Encontramos {len(df_high)} linha(s) com quantidade acima de {HIGH_QTY_THRESHOLD}. "
                    "Confira os itens abaixo; corrija se necessário ou marque a confirmação para continuar."
                )
                st.dataframe(df_high, use_container_width=True)
                confirm_high = st.checkbox(f"Confirmo as quantidades altas (>{HIGH_QTY_THRESHOLD}) apresentadas acima")
            
            # Botão: Simular baixa (só resumo)
            if st.button("🧪 Simular baixa (não grava)"):
                if (edited.get("quantidade_corrigida", 0) <= 0).any():
                    st.error("Há linhas com 'Qtd. corrigida' inválida (<= 0). Corrija antes de simular.")
                else:
                    total_itens = len(preview)
                    total_faltara = int(preview["Faltará"].sum())
                    total_sera_vendido = int(preview["Será vendido"].sum())
                    total_qtd_solicitada = int(preview["Qtd. (usada)"].sum())
                    
                    st.info(
                        f"Simulação: {total_itens} linhas | "
                        f"Total solicitado: {total_qtd_solicitada} | "
                        f"Será vendido: {total_sera_vendido} | "
                        f"Faltará vender: {total_faltara}"
                    )
            
            grava_map = st.checkbox("Salvar/atualizar mapeamentos sku_pdf → sku (para os itens com SKU preenchido)", value=True)
            
            # Botão: Aplicar baixas
            if st.button("Aplicar baixas (venda_pdf)", type="primary"):
                if confirm_high_needed and not confirm_high:
                    st.error(f"Existem quantidades acima de {HIGH_QTY_THRESHOLD} não confirmadas. Confirme ou corrija antes de aplicar.")
                    st.stop()
                
                if edited.empty:
                    st.error("Não há itens para processar.")
                    st.stop()
                
                if (edited.get("quantidade_corrigida", 0) <= 0).any():
                    st.error("Há linhas com 'Qtd. corrigida' inválida (<= 0). Corrija antes de aplicar.")
                    st.stop()
                
                backup_database()
                con = get_conn()
                cur = con.cursor()
                ok_count = 0
                mapeados = 0
                erros = 0
                faltando = 0
                total_faltou_vender = 0
                
                for _, r in edited.iterrows():
                    sku_pdf = sanitize_sku(str(r.get("sku_pdf", "")))
                    
                    # CORREÇÃO: Pegar quantidade corretamente
                    qtd_corrigida = r.get("quantidade_corrigida")
                    if pd.isna(qtd_corrigida) or qtd_corrigida is None:
                        qtd = int(r.get("quantidade", 0) or 0)
                    else:
                        qtd = int(qtd_corrigida)
                    
                    sku_user = str(r.get("sku", ""))
                    sku_est_sanit = sanitize_sku(sku_user)
                    
                    if not sku_est_sanit:
                        faltando += 1
                        continue
                    
                    if sku_est_sanit not in existentes:
                        erros += 1
                        continue
                    
                    sku_original = sku_san_to_orig[sku_est_sanit]
                    
                    # NOVA LÓGICA: Verificar estoque atual e ajustar quantidade se necessário
                    estoque_atual_item = map_estoque.get(sku_original, 0)
                    quantidade_a_baixar = min(qtd, estoque_atual_item)  # Baixa no máximo o estoque disponível
                    faltara_item = max(0, qtd - estoque_atual_item)
                    
                    try:
                        if quantidade_a_baixar > 0:  # Só registra se houver algo para baixar
                            record_movement(sku_original, -quantidade_a_baixar, "venda_pdf")
                            ok_count += 1
                            
                            if faltara_item > 0:
                                total_faltou_vender += faltara_item
                                st.warning(f"SKU {sku_original}: Baixadas {quantidade_a_baixar} unidades (faltou baixar {faltara_item})")
                        
                        if grava_map and sku_pdf:
                            try:
                                cur.execute(
                                    "INSERT INTO sku_mapping(sku_pdf, sku_estoque) VALUES(?, ?) "
                                    "ON CONFLICT(sku_pdf) DO UPDATE SET sku_estoque=excluded.sku_estoque",
                                    (sku_pdf, sku_original)
                                )
                                con.commit()
                                mapeados += 1
                            except Exception:
                                pass
                    except Exception:
                        erros += 1
                
                mensagem_sucesso = f"Baixas aplicadas! OK: {ok_count} | Mapeamentos salvos: {mapeados} | Sem SKU preenchido: {faltando} | Erros: {erros}"
                if total_faltou_vender > 0:
                    mensagem_sucesso += f" | Total que faltou vender: {total_faltou_vender} unidades"
                
                st.success(mensagem_sucesso)
            
            st.divider()
            # Exporta exatamente o que está na grade (inclui quantidade_corrigida)
            st.download_button(
                "📥 Exportar leitura do PDF (CSV)",
                edited.to_csv(index=False).encode("utf-8"),
                "baixa_pdf_preview.csv",
                "text/csv"
            )

# -------- Estoque Atual --------
elif page == "Estoque Atual":
    st.subheader("Estoque atual por SKU")
    f1, f2, f3 = st.columns([2,1,1])
    with f1:
        filtro = st.text_input("Filtro (SKU, categoria, subtipo, cor ou tamanho)")
    with f2:
        critico = st.number_input("Estoque crítico (abaixo de)", min_value=0, value=5, step=1)
    with f3:
        modo_exibicao = st.radio("Modo de exibição", ["Todos os itens", "Apenas críticos"], horizontal=True)
    
    apenas_criticos = (modo_exibicao == "Apenas críticos")
    df = stock_df(filter_text=filtro if filtro else None, critical_only=apenas_criticos, critical_value=critico)
    
    if not df.empty and 'valor_estoque' in df.columns:
        # Calcular apenas itens positivos para o valor total
        df_positivos = df[df['estoque'] > 0]
        valor_total_estoque = df_positivos['valor_estoque'].sum()
        total_itens = len(df)
        total_unidades = df['estoque'].sum()
        total_unidades_positivas = df_positivos['estoque'].sum()
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total de itens", total_itens)
        with col2:
            st.metric("Total de unidades", total_unidades)
        with col3:
            st.metric("Valor total do estoque", f"R$ {valor_total_estoque:,.2f}")
        with col4:
            custo_medio = valor_total_estoque / total_unidades_positivas if total_unidades_positivas > 0 else 0
            st.metric("Custo médio por unidade", f"R$ {custo_medio:.2f}")
    
    if df.empty:
        st.info("Nenhuma variante encontrada.")
    else:
        def highlight(row):
            if row["estoque"] < 0:
                return ["background-color: #ffcccc" for _ in row]
            if row["estoque"] <= critico:
                return ["background-color: #fff2cc" for _ in row]
            return ["" for _ in row]
        
        display_df = df.copy()
        if 'custo_unitario' in display_df.columns:
            display_df['custo_unitario'] = display_df['custo_unitario'].apply(lambda x: f"R$ {x:,.2f}" if pd.notnull(x) else "R$ 0,00")
        if 'valor_estoque' in display_df.columns:
            # Para itens negativos, mostrar valor zero
            display_df['valor_estoque'] = display_df.apply(
                lambda x: "R$ 0,00" if x['estoque'] < 0 else f"R$ {x['valor_estoque']:,.2f}", 
                axis=1
            )
        
        # CORREÇÃO: Resetar índice para evitar erro de índice duplicado
        display_df_reset = display_df.reset_index(drop=True)
        st.dataframe(display_df_reset.style.apply(highlight, axis=1), use_container_width=True, hide_index=True)
        
        total_criticos = len(df[df["estoque"] <= critico])
        total_negativos = len(df[df["estoque"] < 0])
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Itens críticos", total_criticos)
        with col2:
            st.metric("Estoques negativos", total_negativos)

# -------- Histórico --------
elif page == "Histórico":
    st.subheader("Histórico de Movimentações")
    colf1, colf2, colf3 = st.columns([2,1,1])
    with colf1:
        sku_escolhido = st.selectbox("Filtrar por SKU (digite para filtrar)", [""] + list(list_variants_df()["sku"].tolist()), index=0)
    with colf2:
        motivo = st.selectbox("Motivo", ["Todos", "entrada", "venda", "venda_pdf", "ajuste"])
    with colf3:
        dias = st.selectbox("Período", ["Todos", "7", "30", "90"], index=2)
    
    days = None if dias == "Todos" else int(dias)
    dfh = movements_df(sku_filter=sku_escolhido if sku_escolhido else None, reason=motivo if motivo != "Todos" else None, days=days)
    st.dataframe(dfh, use_container_width=True)

# -------- Exportar CSV --------
elif page == "Exportar CSV":
    st.subheader("Exportar dados")
    v = list_variants_df()
    s = stock_df()
    m = movements_df()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.download_button("📥 Variantes (CSV)", v.to_csv(index=False).encode("utf-8"), "variantes.csv", "text/csv")
    with col2:
        st.download_button("📥 Estoque (CSV)", s.to_csv(index=False).encode("utf-8"), "estoque.csv", "text/csv")
    with col3:
        st.download_button("📥 Movimentações (CSV)", m.to_csv(index=False).encode("utf-8"), "movimentacoes.csv", "text/csv")

# -------- Editar Variante --------
elif page == "Editar Variante":
    st.subheader("Editar Variante (com autocomplete de SKU)")
    vdf = list_variants_df()
    current_sku = st.selectbox("Selecione o SKU", vdf["sku"].tolist(), index=None, placeholder="Digite parte do SKU…")
    
    if current_sku:
        det = get_variant_details(current_sku)
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            category = st.text_input("Categoria", det["category"])
        with col2:
            subtype = st.text_input("Subtipo", det["subtype"])
        with col3:
            color = st.text_input("Cor", det["color"])
        with col4:
            size = st.text_input("Tamanho", det["size"])
        with col5:
            sku_base = st.text_input("SKU Base", det["sku_base"] or "")
        
        # Custos
        c1, c2 = st.columns(2)
        with c1:
            custo_unitario_produto = st.number_input("Custo Unitário (PRODUTO) R$", min_value=0.0, value=float(det.get("custo_unitario_produto", 0) or 0), step=0.01, help="Custo padrão do tipo/subtipo. Variantes podem ter custo próprio.")
        with c2:
            cur_val = det.get("custo_unitario_variante", None)
            custo_unitario_variante = st.number_input("Custo Unitário (VARIANTE) R$ (opcional)", min_value=0.0, value=float(cur_val if cur_val is not None else 0.0), step=0.01, help="Se > 0, substitui o custo do produto apenas para esta variante.")
        
        new_sku = st.text_input("Novo SKU", det["sku"])
        
        if st.button("Salvar alterações", type="primary"):
            cvar = custo_unitario_variante if custo_unitario_variante > 0 else None
            ok, msg = update_variant(det["sku"], new_sku, category, subtype, color, size, sku_base if sku_base else None, custo_unitario_produto, cvar)
            if ok:
                st.success(msg)
            else:
                st.error(msg)

# -------- Remover Variante --------
elif page == "Remover Variante":
    st.subheader("Remover Variante")
    vdf = list_variants_df()
    sku = st.selectbox("Selecione o SKU", vdf["sku"].tolist(), index=None, placeholder="Digite para filtrar…")
    
    if st.button("Remover", type="primary"):
        if not sku:
            st.error("Selecione um SKU.")
        else:
            ok, msg = delete_variant(sku)
            st.success(msg) if ok else st.error(msg)

# -------- Mapeamento de SKUs --------
elif page == "Mapeamento de SKUs":
    st.subheader("Mapeamentos (sku_pdf → sku)")
    con = get_conn()
    df_map = pd.read_sql_query("SELECT id, sku_pdf, sku_estoque FROM sku_mapping ORDER BY id DESC", con)
    st.dataframe(df_map, use_container_width=True)
    
    # NOVO: Excluir mapeamento
    st.markdown("### Excluir mapeamento existente")
    if not df_map.empty:
        col_del1, col_del2, col_del3 = st.columns([2,2,1])
        with col_del1:
            del_by = st.radio("Selecionar por", ["ID", "SKU (PDF)"], horizontal=True)
        with col_del2:
            if del_by == "ID":
                sel_id = st.selectbox("ID do mapeamento", df_map["id"].tolist(), index=None, placeholder="Selecione o ID…")
                sel_sku_pdf = None
            else:
                sel_sku_pdf = st.selectbox("SKU (PDF)", df_map["sku_pdf"].tolist(), index=None, placeholder="Selecione o SKU (PDF)…")
                sel_id = None
        with col_del3:
            do_delete = st.button("🗑️ Excluir", type="secondary")
        
        if do_delete:
            try:
                con = get_conn()
                if del_by == "ID" and sel_id is not None:
                    con.execute("DELETE FROM sku_mapping WHERE id=?", (int(sel_id),))
                    con.commit()
                    st.success(f"Mapeamento ID {sel_id} excluído.")
                    st.rerun()
                elif del_by != "ID" and sel_sku_pdf:
                    con.execute("DELETE FROM sku_mapping WHERE sku_pdf=?", (str(sel_sku_pdf),))
                    con.commit()
                    st.success(f"Mapeamento do SKU (PDF) '{sel_sku_pdf}' excluído.")
                    st.rerun()
                else:
                    st.warning("Selecione um item para excluir.")
            except Exception as e:
                st.error(f"Erro ao excluir mapeamento: {e}")
    else:
        st.info("Não há mapeamentos para excluir.")
    
    with st.expander("Adicionar mapeamento manualmente"):
        col1, col2 = st.columns(2)
        with col1:
            sku_pdf = st.text_input("SKU (PDF)")
        with col2:
            sku_estoque = st.selectbox("SKU no estoque", list_variants_df()["sku"].tolist(), index=None, placeholder="Digite para filtrar…")
        
        if st.button("Adicionar mapeamento"):
            if sku_pdf and sku_estoque:
                try:
                    con.execute(
                        "INSERT INTO sku_mapping(sku_pdf, sku_estoque) VALUES(?, ?) "
                        "ON CONFLICT(sku_pdf) DO UPDATE SET sku_estoque=excluded.sku_estoque",
                        (sanitize_sku(sku_pdf), str(sku_estoque))
                    )
                    con.commit()
                    st.success("Mapeamento adicionado/atualizado.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
            else:
                st.error("Preencha os dois campos.")

# -------- Gerenciar SKU Base --------
elif page == "Gerenciar SKU Base":
    st.subheader("Atualizar SKU Base e regenerar SKUs das variantes")
    col1, col2, col3 = st.columns(3)
    with col1:
        category = st.text_input("Categoria")
    with col2:
        subtype = st.text_input("Subtipo")
    with col3:
        new_base = st.text_input("Novo SKU Base (ex.: MOL-CARECA)")
    
    if st.button("Atualizar SKU Base", type="primary"):
        if not (category and subtype and new_base):
            st.error("Preencha categoria, subtipo e novo SKU base.")
        else:
            ok, msg = update_sku_base_bulk(category, subtype, new_base)
            st.success(msg) if ok else st.error(msg)

# -------- Custo por Categoria/Subtipo (em massa) --------
elif page == "Custo por Categoria/Subtipo (em massa)":
    st.subheader("Atualizar Custo Unitário em Massa por Categoria/Subtipo")
    df_produtos = list_products_df()
    
    if df_produtos.empty:
        st.info("Nenhum produto cadastrado ainda.")
    else:
        categorias = sorted(df_produtos["category"].dropna().unique().tolist())
        c1, c2 = st.columns([2, 3])
        with c1:
            categoria_escolhida = st.selectbox("Categoria", [""] + categorias, index=0)
        with c2:
            if categoria_escolhida:
                subtipos_disp = (
                    df_produtos[df_produtos["category"] == categoria_escolhida]["subtype"]
                    .dropna().unique().tolist()
                )
                subtipos_disp = sorted(subtipos_disp)
                subtipos_escolhidos = st.multiselect(
                    "Subtipos (se vazio, aplica em TODOS os subtipos da categoria)",
                    subtipos_disp,
                    default=subtipos_disp
                )
            else:
                subtipos_escolhidos = []
        
        novo_custo = st.number_input(
            "Novo Custo Unitário (PRODUTO) R$",
            min_value=0.0,
            value=0.0,
            step=0.01,
            help="Atualiza o custo padrão do produto. Variantes com custo próprio não são afetadas."
        )
        
        afetadas = 0
        if categoria_escolhida:
            df_alvo = df_produtos[df_produtos["category"] == categoria_escolhida]
            if subtipos_escolhidos:
                df_alvo = df_alvo[df_alvo["subtype"].isin(subtipos_escolhidos)]
            vdf = list_variants_df()
            prod_ids = df_alvo["id"].tolist()
            afetadas = len(vdf[vdf["product_id"].isin(prod_ids)]) if "product_id" in vdf.columns else 0
            st.caption(f"Variantes impactadas (estimativa): **{afetadas}** (apenas no custo padrão; variantes com custo próprio continuam com o seu valor)")
        
        colb1, colb2 = st.columns([1, 2])
        with colb1:
            aplicar = st.button("Aplicar custo em massa", type="primary")
        
        if aplicar:
            if not categoria_escolhida:
                st.error("Escolha uma categoria.")
            elif novo_custo <= 0:
                st.error("Informe um custo maior que zero.")
            else:
                alvo = df_produtos[df_produtos["category"] == categoria_escolhida]
                if subtipos_escolhidos:
                    alvo = alvo[alvo["subtype"].isin(subtipos_escolhidos)]
                
                if alvo.empty:
                    st.warning("Não há produtos para atualizar com os filtros escolhidos.")
                else:
                    ok_cnt, err_cnt = 0, 0
                    for _, row in alvo.iterrows():
                        ok, msg = update_custo_unitario(row["category"], row["subtype"], float(novo_custo))
                        if ok:
                            ok_cnt += 1
                        else:
                            err_cnt += 1
                            st.warning(f"{row['category']} / {row['subtype']}: {msg}")
                    
                    st.success(f"Custo padrão atualizado para R$ {novo_custo:.2f} em {ok_cnt} produto(s).")
                    if err_cnt:
                        st.error(f"Ocorreu erro em {err_cnt} produto(s).")

# -------- Contagem de Estoque --------
elif page == "Contagem de Estoque":
    st.subheader("Contagem de Estoque (ajuste por inventário)")
    vdf = list_variants_df()
    sku = st.selectbox("SKU", vdf["sku"].tolist(), index=None, placeholder="Digite para filtrar…")
    
    if sku:
        atual = stock_df(filter_text=sku)
        saldo_atual = int(atual.loc[atual["sku"] == sku, "estoque"].values[0]) if not atual.empty else 0
        novo = st.number_input("Quantidade contada (substitui o saldo)", value=saldo_atual, step=1)
        
        if st.button("Aplicar contagem", type="primary"):
            delta = novo - saldo_atual
            if delta != 0:
                record_movement(sku, int(delta), "ajuste")
                st.success(f"Saldo ajustado. Anterior: {saldo_atual} | Novo: {novo}")

# -------- Valor do Estoque (CORRIGIDO) --------
elif page == "Valor do Estoque":
    st.subheader("💰 Valor Total do Estoque (Apenas Itens Positivos)")
    
    # Adicionar toggle para escolher entre visualização
    col1, col2, col3 = st.columns([2, 2, 2])
    with col1:
        filtro_categoria = st.text_input("Filtrar por categoria", placeholder="Ex: moletom, camiseta")
    with col2:
        filtro_subtipo = st.text_input("Filtrar por subtipo", placeholder="Ex: canguru, careca")
    with col3:
        mostrar_negativos = st.checkbox("Mostrar itens negativos", value=False, 
                                       help="Mostra itens com estoque negativo (não afetam o valor total)")
    
    # Obter dados (apenas positivos para cálculos)
    df_positivo = stock_value_positive_df()
    
    # Aplicar filtros
    if filtro_categoria:
        df_positivo = df_positivo[df_positivo['category'].str.contains(filtro_categoria, case=False, na=False)]
    if filtro_subtipo:
        df_positivo = df_positivo[df_positivo['subtype'].str.contains(filtro_subtipo, case=False, na=False)]
    
    # Obter dados completos se necessário para mostrar negativos
    if mostrar_negativos:
        df_completo = stock_value_df()
        if filtro_categoria:
            df_completo = df_completo[df_completo['category'].str.contains(filtro_categoria, case=False, na=False)]
        if filtro_subtipo:
            df_completo = df_completo[df_completo['subtype'].str.contains(filtro_subtipo, case=False, na=False)]
        df_negativo = df_completo[df_completo['estoque'] < 0]
    else:
        df_negativo = pd.DataFrame()
    
    if df_positivo.empty and (not mostrar_negativos or df_negativo.empty):
        st.info("Nenhum item encontrado com os filtros aplicados.")
    else:
        # Calcular totais CORRETOS (apenas positivos)
        valor_total = df_positivo['valor_estoque'].sum()
        total_itens_positivos = len(df_positivo)
        total_unidades_positivas = df_positivo['estoque'].sum()
        
        # Estatísticas de negativos
        total_itens_negativos = len(df_negativo) if mostrar_negativos else 0
        total_unidades_negativas = abs(df_negativo['estoque'].sum()) if mostrar_negativos and not df_negativo.empty else 0
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Valor Total do Estoque", f"R$ {valor_total:,.2f}")
        with col2:
            st.metric("Itens com Estoque Positivo", total_itens_positivos)
        with col3:
            st.metric("Unidades em Estoque", f"{total_unidades_positivas:,.0f}")
        with col4:
            custo_medio = valor_total / total_unidades_positivas if total_unidades_positivas > 0 else 0
            st.metric("Custo Médio por Unidade", f"R$ {custo_medio:.2f}")
        
        # Alertas para itens negativos
        if total_itens_negativos > 0:
            st.warning(f"⚠️ **Atenção:** {total_itens_negativos} itens com estoque negativo ({total_unidades_negativas} unidades em falta)")
            
            # Mostrar itens negativos em uma tabela expandida
            with st.expander("📋 Ver Itens com Estoque Negativo"):
                df_negativo_display = df_negativo.copy()
                df_negativo_display['estoque'] = df_negativo_display['estoque'].astype(int)
                df_negativo_display['valor_estoque'] = "R$ 0,00"
                st.dataframe(df_negativo_display[['sku', 'category', 'subtype', 'color', 'size', 'estoque']], 
                           use_container_width=True)
        
        st.divider()
        st.subheader("Valor por Categoria/Subtipo (Apenas Positivos)")
        df_agrupado = df_positivo.groupby(['category', 'subtype']).agg({
            'estoque': 'sum',
            'valor_estoque': 'sum',
            'sku': 'count'
        }).reset_index().rename(columns={'sku':'quantidade_skus','estoque':'total_unidades'}).sort_values('valor_estoque', ascending=False)
        
        disp = df_agrupado.copy()
        disp['valor_estoque'] = disp['valor_estoque'].apply(lambda x: f"R$ {x:,.2f}")
        st.dataframe(disp, use_container_width=True)
        
        st.divider()
        st.subheader("Detalhamento Completo do Estoque")
        
        # Preparar dados para exibição
        if mostrar_negativos:
            df_exibicao = pd.concat([df_positivo, df_negativo]).sort_values(['category', 'subtype', 'color', 'size'])
        else:
            df_exibicao = df_positivo
        
        # Função para destacar itens negativos
        def highlight_negative(row):
            if row['estoque'] < 0:
                return ['background-color: #ffcccc'] * len(row)
            return [''] * len(row)
        
        detalhado = df_exibicao.copy()
        detalhado['custo_unitario'] = detalhado['custo_unitario'].apply(lambda x: f"R$ {x:,.2f}" if pd.notnull(x) else "R$ 0,00")
        # Para itens negativos, mostrar valor zero
        detalhado['valor_estoque'] = detalhado.apply(
            lambda x: "R$ 0,00" if x['estoque'] < 0 else f"R$ {x['valor_estoque']:,.2f}", 
            axis=1
        )
        
        # CORREÇÃO: Resetar o índice para evitar o erro de índice duplicado
        detalhado_reset = detalhado.reset_index(drop=True)
        
        # Aplicar o estilo no DataFrame com índice resetado
        styled_df = detalhado_reset.style.apply(highlight_negative, axis=1)
        st.dataframe(styled_df, use_container_width=True)
        
        # Exportar dados
        csv = df_exibicao.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Exportar Dados de Valor do Estoque (CSV)", csv, "valor_estoque.csv", "text/csv")

# -------- Gráfico de Vendas --------
elif page == "Gráfico de Vendas":
    st.subheader("📊 Gráfico de Vendas")
    coltop1, coltop2, coltop3 = st.columns(3)
    with coltop1:
        periodo = st.selectbox(
            "Período",
            ["Últimos 7 dias", "Últimos 30 dias", "Últimos 90 dias", "Todo o período"],
            index=1
        )
    with coltop2:
        limite_produtos = st.slider("Nº no ranking de produtos", 5, 30, 10)
    with coltop3:
        modo_valor = st.selectbox("Métrica financeira", ["Valor ao Custo", "Somente Quantidade"], index=0)
    
    dias_map = {"Últimos 7 dias": 7, "Últimos 30 dias": 30, "Últimos 90 dias": 90, "Todo o período": None}
    dias = dias_map[periodo]
    df_vendas = get_sales_data(days=dias)
    
    if df_vendas.empty:
        st.info("Nenhuma venda no período.")
    else:
        st.markdown("### Filtros por Produto")
        f1, f2 = st.columns(2)
        with f1:
            filtro_cat = st.text_input("Categoria (ex.: MOLETOM, CAMISETA)", value="")
        with f2:
            filtro_sub = st.text_input("Subtipo (ex.: CARECA, CANGURU)", value="")
        
        df_prod = df_vendas.copy()
        if filtro_cat:
            df_prod = df_prod[df_prod["category"].str.contains(filtro_cat, case=False, na=False)]
        if filtro_sub:
            df_prod = df_prod[df_prod["subtype"].str.contains(filtro_sub, case=False, na=False)]
        
        total_qtd = int(df_prod["quantidade_vendida"].sum())
        total_val = float(df_prod["valor_total_vendido"].sum())
        total_regs = int(df_prod["numero_vendas"].sum())
        
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Unidades vendidas", total_qtd)
        with m2:
            st.metric("Valor ao custo", f"R$ {total_val:,.2f}")
        with m3:
            st.metric("Registros (linhas) de venda", total_regs)
        
        st.divider()
        st.markdown("### Top Produtos (Categoria-Subtipo)")
        df_top = (
            df_prod.groupby(["category", "subtype"], as_index=False)[["quantidade_vendida", "valor_total_vendido"]]
            .sum()
            .assign(produto=lambda d: d["category"] + " - " + d["subtype"])
            .sort_values("quantidade_vendida", ascending=False)
            .head(limite_produtos)
        )
        
        if not df_top.empty:
            fig1 = px.bar(
                df_top,
                x="quantidade_vendida",
                y="produto",
                orientation="h",
                title=f"Top {limite_produtos} por Quantidade",
                labels={"quantidade_vendida":"Quantidade","produto":"Produto"}
            )
            fig1.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig1, use_container_width=True)
            
            if modo_valor == "Valor ao Custo":
                fig2 = px.bar(
                    df_top.sort_values("valor_total_vendido"),
                    x="valor_total_vendido",
                    y="produto",
                    orientation="h",
                    title=f"Top {limite_produtos} por Valor (Custo)",
                    labels={"valor_total_vendido":"Valor (R$)","produto":"Produto"}
                )
                fig2.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig2, use_container_width=True)
        
        st.divider()
        st.markdown("### Tamanhos mais vendidos (no produto filtrado)")
        if not df_prod.empty:
            df_tam = (
                df_prod.groupby("size", as_index=False)[["quantidade_vendida","valor_total_vendido"]]
                .sum().sort_values("quantidade_vendida", ascending=False).head(30)
            )
            fig_tam = px.bar(
                df_tam,
                x="size",
                y="quantidade_vendida",
                title="Top Tamanhos (por quantidade) — respeitando filtros de Categoria/Subtipo",
                labels={"size":"Tamanho","quantidade_vendida":"Quantidade"}
            )
            st.plotly_chart(fig_tam, use_container_width=True)
        else:
            st.info("Aplique filtros de Categoria/Subtipo para ver tamanhos específicos do produto.")
        
        st.divider()
        st.markdown("### Top Itens (Categoria-Subtipo-Cor-Tamanho)")
        df_itens = (
            df_prod
            .assign(
                item=lambda d: (
                    d["category"].astype(str).str.upper().str.replace(r"\s+","-", regex=True) + "-" +
                    d["subtype"].astype(str).str.upper().str.replace(r"\s+","-", regex=True) + "-" +
                    d["color"].astype(str).str.upper().str.replace(r"\s+","-", regex=True) + "-" +
                    d["size"].astype(str).str.upper()
                )
            )
            .groupby("item", as_index=False)[["quantidade_vendida","valor_total_vendido"]]
            .sum().sort_values("quantidade_vendida", ascending=False)
        )
        
        df_itens = df_itens.sort_values("quantidade_vendida", ascending=False)
        n_itens = st.slider("Quantos itens mostrar no ranking?", 5, 100, 20, key="slider_top_itens")
        top_itens = df_itens.head(n_itens)
        
        cti1, cti2 = st.columns(2)
        with cti1:
            fig_items_q = px.bar(
                top_itens.sort_values("quantidade_vendida"),
                x="quantidade_vendida",
                y="item",
                orientation="h",
                title=f"Top {n_itens} Itens por Quantidade",
                labels={"quantidade_vendida":"Quantidade","item":"Item"}
            )
            fig_items_q.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig_items_q, use_container_width=True)
        
        with cti2:
            fig_items_v = px.bar(
                top_itens.sort_values("valor_total_vendido"),
                x="valor_total_vendido",
                y="item",
                orientation="h",
                title=f"Top {n_itens} Itens por Valor (ao custo)",
                labels={"valor_total_vendido":"Valor (R$)","item":"Item"}
            )
            fig_items_v.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig_items_v, use_container_width=True)
        
        st.dataframe(top_itens, use_container_width=True)
        st.download_button(
            "📥 Exportar Top Itens (CSV)",
            df_itens.to_csv(index=False).encode("utf-8"),
            "ranking_top_itens.csv",
            "text/csv"
        )
        
        st.divider()
        if not filtro_cat and not filtro_sub:
            st.markdown("### Distribuição por Categoria (geral)")
            df_cat = (
                df_vendas.groupby('category')
                .agg({'quantidade_vendida':'sum','valor_total_vendido':'sum'})
                .reset_index().sort_values('quantidade_vendida', ascending=False)
            )
            c1, c2 = st.columns(2)
            with c1:
                st.plotly_chart(px.pie(df_cat, values='quantidade_vendida', names='category', title='Vendas por Categoria (Qtd)'), use_container_width=True)
            with c2:
                st.plotly_chart(px.pie(df_cat, values='valor_total_vendido', names='category', title='Vendas por Categoria (Valor)'), use_container_width=True)
        
        st.divider()
        st.download_button(
            "📥 Exportar Dados de Vendas (CSV — filtros aplicados)",
            df_prod.to_csv(index=False).encode("utf-8"),
            f"vendas_{periodo.lower().replace(' ','_')}_filtrado.csv",
            "text/csv"
        )

# -------- Rodapé --------
st.divider()
st.caption("© Controle de Estoque — feito com Streamlit + SQLite. Auditoria por movimentação e saldo por SKU.")