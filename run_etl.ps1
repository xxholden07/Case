Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  ETL - Vendas Analytics" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 1. Verificar arquivos de entrada
Write-Host "[1/3] Verificando arquivos..." -ForegroundColor Yellow

if (-Not (Test-Path ".\input\sales.csv")) {
    Write-Host "  → sales.csv nao encontrado, convertendo..." -ForegroundColor Yellow
    python -c "import pandas as pd; df = pd.read_excel('sales.xlsx'); df.to_csv('input/sales.csv', index=False); print('OK')"
    Write-Host "  ✓ Conversao concluida" -ForegroundColor Green
} else {
    Write-Host "  ✓ sales.csv encontrado" -ForegroundColor Green
}

Write-Host ""

# 2. Configurar ambiente e executar Spark
Write-Host "[2/3] Executando analise PySpark..." -ForegroundColor Yellow

$env:JAVA_HOME = "C:\Users\matheus.rodrigues\AppData\Local\Programs\Microsoft\jdk-17.0.13.11-hotspot"
$env:PYSPARK_PYTHON = "python"

python .\scripts\spark_analysis.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "  ✗ Erro na analise Spark" -ForegroundColor Red
    exit 1
}

Write-Host "  ✓ Analise concluida" -ForegroundColor Green
Write-Host ""

# 3. Exportar para SQLite
Write-Host "[3/4] Exportando para SQLite..." -ForegroundColor Yellow

python .\scripts\save_to_sqlite.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "  ✗ Erro na exportacao SQLite" -ForegroundColor Red
    exit 1
}

Write-Host "  ✓ Exportacao concluida" -ForegroundColor Green
Write-Host ""

# 4. Enriquecer CEP via API ViaCEP
Write-Host "[4/4] Enriquecendo dados de CEP..." -ForegroundColor Yellow

python .\scripts\enriquecer_cep.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "  ✗ Erro no enriquecimento de CEP" -ForegroundColor Red
    exit 1
}

Write-Host "  ✓ Enriquecimento concluido" -ForegroundColor Green
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  ETL Concluido!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Proximo passo: .\dashboard\run_dashboard.ps1" -ForegroundColor Yellow
