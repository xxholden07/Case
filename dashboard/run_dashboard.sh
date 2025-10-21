#!/bin/bash

echo ""
echo "INICIANDO DASHBOARD STREAMLIT"
echo ""

# Verificar se o banco existe
if [ ! -f "../output/vendas_analytics.db" ]; then
    echo "❌ Banco SQLite não encontrado."
    echo "Execute primeiro: ../run_etl.sh"
    exit 1
fi

# Verificar dependências
echo "Verificando dependências..."
python -c "import streamlit, plotly" 2>/dev/null

if [ $? -ne 0 ]; then
    echo "Instalando Streamlit e Plotly..."
    pip install streamlit plotly
fi

echo ""
echo "✓ Iniciando dashboard..."
echo "O navegador abrirá automaticamente"
echo ""

# Executar Streamlit
streamlit run app_streamlit.py
