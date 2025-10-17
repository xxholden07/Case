Write-Host ""
Write-Host "INICIANDO DASHBOARD STREAMLIT" -ForegroundColor Cyan
Write-Host ""

# Verificar se o banco existe (agora em output/)
if (-Not (Test-Path "..\output\vendas_analytics.db")) {
    Write-Host "Banco SQLite nao encontrado. Execute primeiro: .\run_etl.ps1" -ForegroundColor Red
    exit 1
}

# Instalar streamlit e plotly se necessario
Write-Host "Verificando dependencias..." -ForegroundColor Yellow
python -c "import streamlit, plotly" 2>$null

if ($LASTEXITCODE -ne 0) {
    Write-Host "Instalando Streamlit e Plotly..." -ForegroundColor Yellow
    pip install streamlit plotly
}

Write-Host ""
Write-Host "Iniciando dashboard..." -ForegroundColor Green
Write-Host "O navegador abrira automaticamente" -ForegroundColor Green
Write-Host ""

# Executar Streamlit
streamlit run app_streamlit.py
