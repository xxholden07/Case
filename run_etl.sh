#!/bin/bash

echo "========================================"
echo "  ETL - Vendas Analytics"
echo "========================================"
echo ""

# 1. Verificar arquivos de entrada
echo "[1/3] Verificando arquivos..."

if [ ! -f "./input/sales.csv" ]; then
    echo "  → sales.csv não encontrado, convertendo..."
    python -c "import pandas as pd; df = pd.read_excel('sales.xlsx'); df.to_csv('input/sales.csv', index=False); print('OK')"
    if [ $? -eq 0 ]; then
        echo "  ✓ Conversão concluída"
    else
        echo "  ✗ Erro na conversão"
        exit 1
    fi
else
    echo "  ✓ sales.csv encontrado"
fi

echo ""

# 2. Configurar ambiente e executar Spark
echo "[2/3] Executando análise PySpark..."

export JAVA_HOME="/usr/lib/jvm/java-21-openjdk-amd64"
export PYSPARK_PYTHON="python"

python ./scripts/spark_analysis.py

if [ $? -ne 0 ]; then
    echo "  ✗ Erro na análise Spark"
    exit 1
fi

echo "  ✓ Análise concluída"
echo ""

# 3. Exportar para SQLite
echo "[3/4] Exportando para SQLite..."

python ./scripts/save_to_sqlite.py

if [ $? -ne 0 ]; then
    echo "  ✗ Erro na exportação SQLite"
    exit 1
fi

echo "  ✓ Exportação concluída"
echo ""

# 4. Enriquecer CEP via API ViaCEP
echo "[4/4] Enriquecendo dados de CEP..."

python ./scripts/enriquecer_cep.py

if [ $? -ne 0 ]; then
    echo "  ✗ Erro no enriquecimento de CEP"
    exit 1
fi

echo "  ✓ Enriquecimento concluído"
echo ""
echo "========================================"
echo "  ETL Concluído!"
echo "========================================"
echo ""
echo "Próximo passo: ./dashboard/run_dashboard.sh"
