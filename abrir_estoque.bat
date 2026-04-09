@echo off
chcp 65001 > nul
title Estoque JIOR BLANC
echo ===============================
echo    INICIANDO ESTOQUE JIOR BLANC
echo ===============================
echo.

cd /d "C:\Users\Rafael Cintra\Desktop\Estoque - Teste"

if exist app.py (
    echo Aplicacao encontrada. Iniciando...
    echo.
    echo Aguarde alguns segundos...
    echo O navegador abrira automaticamente.
    echo.
    echo Para fechar: Ctrl+C no terminal
    echo ===============================
    echo.
    streamlit run app.py
) else (
    echo ERRO: Arquivo app.py nao encontrado!
    echo Verifique o caminho: C:\Users\Rafael Cintra\Desktop\Estoque - Teste
    pause
)