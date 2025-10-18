"""
Dashboard de Analytics de Vendas e Metas
Interface Streamlit com Storytelling
"""
import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Configuração da página
st.set_page_config(
    page_title="Analytics de Vendas",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS customizados
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #2c3e50;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: bold;
    }
    .metric-label {
        font-size: 1rem;
        opacity: 0.9;
    }
    .story-box {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 5px solid #1f77b4;
        margin: 1rem 0;
    }
    .insight-box {
        background-color: #e8f5e9;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #4caf50;
        margin: 0.5rem 0;
    }
    .warning-box {
        background-color: #fff3e0;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #ff9800;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Conexão com banco SQLite
@st.cache_resource
def get_connection():
    import os
    # Caminho relativo ao diretório do projeto
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output", "vendas_analytics.db")
    return sqlite3.connect(db_path, check_same_thread=False)

# Funções de carregamento de dados
@st.cache_data
def load_visao_consolidada():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM visao_consolidada", conn)
    # Converter colunas numéricas para float
    numeric_cols = ['volume_medio_3m', 'volume_medio_6m', 'share_b2b_3m', 'share_b2b_6m']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

@st.cache_data
def load_metas_por_cliente():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM metas_por_cliente", conn)
    return df

@st.cache_data
def load_dim_cliente():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM dim_cliente", conn)
    return df

@st.cache_data
def load_dim_produto():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM dim_produto", conn)
    return df

@st.cache_data
def load_fato_vendas():
    conn = get_connection()
    # Join com dim_produto para ter a marca
    query = """
    SELECT f.*, p.marca
    FROM fato_vendas f
    LEFT JOIN dim_produto p ON f.sk_produto = p.sk_produto
    """
    df = pd.read_sql(query, conn)
    return df

# Header Principal
st.markdown('<div class="main-header">Analytics de Vendas e Distribuição de Metas</div>', unsafe_allow_html=True)
st.markdown(f"<p style='text-align: center; color: #7f8c8d;'>Atualizado em {datetime.now().strftime('%d/%m/%Y às %H:%M')}</p>", unsafe_allow_html=True)

# Sidebar para navegação
st.sidebar.title("Navegação")
page = st.sidebar.radio(
    "Escolha a seção:",
    ["Panorama Geral", "Análise de Clientes", "Distribuição de Metas", "Performance por Marca", "Dados Detalhados"]
)

# ============================================================================
# PÁGINA 1: PANORAMA GERAL
# ============================================================================
if page == "Panorama Geral":
    
    # Carregar dados
    fato = load_fato_vendas()
    visao = load_visao_consolidada()
    metas = load_metas_por_cliente()
    produtos = load_dim_produto()
    
    # Filtros
    st.markdown('<div class="sub-header">Filtros</div>', unsafe_allow_html=True)
    col_f1, col_f2 = st.columns(2)
    
    with col_f1:
        marcas_todas = ['Todas'] + sorted(fato['marca'].dropna().unique().tolist())
        marca_filtro_geral = st.multiselect("Filtrar Marcas", marcas_todas, default=['Todas'])
    
    with col_f2:
        status_b2b = st.selectbox("Status B2B", ['Todos', 'B2B', 'B2C'])
    
    # Aplicar filtros
    fato_filtrado = fato.copy()
    if 'Todas' not in marca_filtro_geral and len(marca_filtro_geral) > 0:
        fato_filtrado = fato_filtrado[fato_filtrado['marca'].isin(marca_filtro_geral)]
    if status_b2b == 'B2B':
        fato_filtrado = fato_filtrado[fato_filtrado['b2b_status'] == 1]
    elif status_b2b == 'B2C':
        fato_filtrado = fato_filtrado[fato_filtrado['b2b_status'] == 0]
    
    # Métricas principais
    st.markdown('<div class="sub-header">Indicadores-Chave de Performance</div>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_transacoes = len(fato_filtrado)
        st.metric("Total de Transações", f"{total_transacoes:,}")
    
    with col2:
        total_clientes = visao['cliente_id'].nunique()
        st.metric("Clientes Únicos", f"{total_clientes}")
    
    with col3:
        total_volume = fato_filtrado['volume_hl'].sum()
        st.metric("Volume Total (HL)", f"{total_volume:,.0f}")
    
    with col4:
        total_valor = fato_filtrado['valor'].sum()
        st.metric("Faturamento Total", f"R$ {total_valor:,.2f}")
    
    # Gráfico: Distribuição de Volume por Marca
    st.markdown('<div class="sub-header">Distribuição de Volume por Marca</div>', unsafe_allow_html=True)
    
    volume_marca = fato_filtrado.groupby('marca')['volume_hl'].sum().reset_index()
    volume_marca = volume_marca.sort_values('volume_hl', ascending=False).head(10)
    
    fig1 = px.bar(
        volume_marca,
        x='marca',
        y='volume_hl',
        title='Top 10 Marcas por Volume (HL)',
        labels={'marca': 'Marca', 'volume_hl': 'Volume (HL)'},
        color='volume_hl',
        color_continuous_scale='Blues'
    )
    fig1.update_layout(showlegend=False, height=400)
    st.plotly_chart(fig1, use_container_width=True)
    
    # Gráfico: Distribuição de Faturamento
    st.markdown('<div class="sub-header">Análise de Faturamento</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        faturamento_marca = fato_filtrado.groupby('marca')['valor'].sum().reset_index()
        faturamento_marca = faturamento_marca.sort_values('valor', ascending=False).head(10)
        
        fig2 = px.pie(
            faturamento_marca,
            values='valor',
            names='marca',
            title='Top 10 Marcas por Faturamento'
        )
        fig2.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig2, use_container_width=True)
    
    with col2:
        # Status B2B vs B2C
        status_count = fato_filtrado['b2b_status'].value_counts().reset_index()
        status_count.columns = ['Status', 'Quantidade']
        status_count['Status'] = status_count['Status'].map({1: 'B2B', 0: 'B2C'})
        
        fig3 = px.bar(
            status_count,
            x='Status',
            y='Quantidade',
            title='Distribuição B2B vs B2C',
            color='Status',
            color_discrete_map={'B2B': '#2ecc71', 'B2C': '#e74c3c'}
        )
        st.plotly_chart(fig3, use_container_width=True)

# ============================================================================
# PÁGINA 2: ANÁLISE DE CLIENTES
# ============================================================================
elif page == "Análise de Clientes":
    
    # Carregar dados
    visao = load_visao_consolidada()
    fato = load_fato_vendas()
    
    # Filtros
    st.markdown('<div class="sub-header">Filtros</div>', unsafe_allow_html=True)
    col_f1, col_f2 = st.columns(2)
    
    with col_f1:
        volume_min = st.number_input("Volume Médio 3M Mínimo (HL)", min_value=0.0, value=0.0, step=10.0)
    
    with col_f2:
        top_n = st.slider("Quantidade de Clientes a Exibir", min_value=5, max_value=50, value=20, step=5)
    
    # Top Clientes por Volume
    st.markdown('<div class="sub-header">Top Clientes por Volume Médio (3 meses)</div>', unsafe_allow_html=True)
    
    # Remover nulls e converter para numérico, depois ordenar e filtrar
    visao_filtrada = visao[visao['volume_medio_3m'].notna()].copy()
    visao_filtrada['volume_medio_3m'] = pd.to_numeric(visao_filtrada['volume_medio_3m'], errors='coerce')
    visao_filtrada = visao_filtrada[visao_filtrada['volume_medio_3m'].notna()]
    visao_filtrada = visao_filtrada[visao_filtrada['volume_medio_3m'] >= volume_min]
    top_clientes = visao_filtrada.nlargest(top_n, 'volume_medio_3m')
    
    fig4 = px.bar(
        top_clientes,
        x='cliente_id',
        y='volume_medio_3m',
        title='Volume Médio nos Últimos 3 Meses (HL)',
        labels={'cliente_id': 'Cliente ID', 'volume_medio_3m': 'Volume Médio 3M (HL)'},
        color='volume_medio_3m',
        color_continuous_scale='Viridis'
    )
    fig4.update_layout(showlegend=False, height=400)
    st.plotly_chart(fig4, use_container_width=True)
    
    # Comparação 3M vs 6M
    st.markdown('<div class="sub-header">Comparação: Desempenho 3 Meses vs 6 Meses</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        if 'volume_medio_6m' in visao_filtrada.columns:
            fig5 = px.scatter(
                visao_filtrada,
                x='volume_medio_3m',
                y='volume_medio_6m',
                title='Volume Médio: 3M vs 6M',
                labels={'volume_medio_3m': 'Volume 3M (HL)', 'volume_medio_6m': 'Volume 6M (HL)'},
                trendline='ols'
            )
            st.plotly_chart(fig5, use_container_width=True)
    
    with col2:
        if 'share_b2b_3m' in visao_filtrada.columns and 'share_b2b_6m' in visao_filtrada.columns:
            fig6 = px.scatter(
                visao_filtrada,
                x='share_b2b_3m',
                y='share_b2b_6m',
                title='Share B2B: 3M vs 6M',
                labels={'share_b2b_3m': 'Share B2B 3M (%)', 'share_b2b_6m': 'Share B2B 6M (%)'}
            )
            st.plotly_chart(fig6, use_container_width=True)
    
    # Tabela de clientes
    st.markdown('<div class="sub-header">Detalhamento dos Top Clientes</div>', unsafe_allow_html=True)
    
    display_cols = ['cliente_id', 'data_ultima_compra', 'volume_medio_3m', 'volume_medio_6m', 'share_b2b_3m', 'share_b2b_6m']
    available_cols = [col for col in display_cols if col in top_clientes.columns]
    
    st.dataframe(
        top_clientes[available_cols].style.format({
            'volume_medio_3m': '{:.2f} HL',
            'volume_medio_6m': '{:.2f} HL',
            'share_b2b_3m': '{:.2f}%',
            'share_b2b_6m': '{:.2f}%'
        }),
        use_container_width=True,
        column_config={
            "cliente_id": "ID Cliente",
            "data_ultima_compra": "Última Compra",
            "volume_medio_3m": "Volume 3M",
            "volume_medio_6m": "Volume 6M",
            "share_b2b_3m": "Share B2B 3M",
            "share_b2b_6m": "Share B2B 6M"
        }
    )

# ============================================================================
# PÁGINA 3: DISTRIBUIÇÃO DE METAS
# ============================================================================
elif page == "Distribuição de Metas":
    
    # Carregar dados
    metas = load_metas_por_cliente()
    
    # Filtros
    st.markdown('<div class="sub-header">Filtros</div>', unsafe_allow_html=True)
    col_f1, col_f2 = st.columns(2)
    
    with col_f1:
        marcas_disponiveis = ['Todas'] + sorted(metas['marca'].unique().tolist())
        marca_filtro = st.selectbox("Filtrar por Marca", marcas_disponiveis)
    
    with col_f2:
        meta_min = st.number_input("Meta Mínima por Cliente (HL)", min_value=0.0, value=0.0, step=50.0)
    
    # Aplicar filtros
    metas_filtradas = metas.copy()
    if marca_filtro != 'Todas':
        metas_filtradas = metas_filtradas[metas_filtradas['marca'] == marca_filtro]
    metas_filtradas = metas_filtradas[metas_filtradas['meta_cliente'] >= meta_min]
    
    # Métricas de Metas
    st.markdown('<div class="sub-header">Resumo das Metas 2025</div>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_meta = metas_filtradas['meta_cliente'].sum()
        st.metric("Meta Total Distribuída", f"{total_meta:,.2f} HL")
    
    with col2:
        num_clientes_com_meta = metas_filtradas['cliente_id'].nunique()
        st.metric("Clientes com Meta", f"{num_clientes_com_meta}")
    
    with col3:
        num_marcas = metas['marca'].nunique()
        st.metric("Marcas com Meta", f"{num_marcas}")
    
    # Validação de Metas por Marca
    st.markdown('<div class="sub-header">Validação: Meta por Marca</div>', unsafe_allow_html=True)
    
    validacao = metas.groupby('marca').agg({
        'meta_marca': 'first',
        'meta_cliente': 'sum'
    }).reset_index()
    validacao.columns = ['Marca', 'Meta Original', 'Soma Metas Clientes']
    validacao['Diferença'] = validacao['Meta Original'] - validacao['Soma Metas Clientes']
    validacao['Diferença %'] = (validacao['Diferença'] / validacao['Meta Original'] * 100).round(2)
    
    fig7 = px.bar(
        validacao,
        x='Marca',
        y=['Meta Original', 'Soma Metas Clientes'],
        title='Comparação: Meta Original vs Soma das Metas dos Clientes',
        labels={'value': 'Volume (HL)', 'variable': 'Tipo'},
        barmode='group'
    )
    st.plotly_chart(fig7, use_container_width=True)
    
    # Distribuição de Metas por Cliente
    st.markdown('<div class="sub-header">Top 20 Clientes por Meta Total</div>', unsafe_allow_html=True)
    
    meta_por_cliente = metas_filtradas.groupby('cliente_id')['meta_cliente'].sum().reset_index()
    meta_por_cliente = meta_por_cliente.sort_values('meta_cliente', ascending=False).head(20)
    
    fig8 = px.bar(
        meta_por_cliente,
        x='cliente_id',
        y='meta_cliente',
        title='Meta Total por Cliente (Top 20)',
        labels={'cliente_id': 'Cliente ID', 'meta_cliente': 'Meta Total (HL)'},
        color='meta_cliente',
        color_continuous_scale='RdYlGn'
    )
    st.plotly_chart(fig8, use_container_width=True)
    
    # Participação percentual
    st.markdown('<div class="sub-header">Análise de Participação dos Clientes</div>', unsafe_allow_html=True)
    
    top_participacao = metas_filtradas.nlargest(15, 'percentual_participacao')[['cliente_id', 'marca', 'percentual_participacao', 'meta_cliente']]
    
    fig9 = px.treemap(
        metas_filtradas,
        path=['marca', 'cliente_id'],
        values='meta_cliente',
        title='Hierarquia: Marca → Cliente',
        color='percentual_participacao',
        color_continuous_scale='Blues'
    )
    st.plotly_chart(fig9, use_container_width=True)

# ============================================================================
# PÁGINA 4: PERFORMANCE POR MARCA
# ============================================================================
elif page == "Performance por Marca":
    
    # Carregar dados
    fato = load_fato_vendas()
    metas = load_metas_por_cliente()
    
    # Análise consolidada por marca
    marca_analysis = fato.groupby('marca').agg({
        'volume_hl': 'sum',
        'valor': 'sum',
        'sk_cliente': 'nunique'
    }).reset_index()
    marca_analysis.columns = ['Marca', 'Volume Total (HL)', 'Faturamento Total', 'Num Clientes']
    marca_analysis['Ticket Médio'] = marca_analysis['Faturamento Total'] / marca_analysis['Volume Total (HL)']
    marca_analysis = marca_analysis.sort_values('Volume Total (HL)', ascending=False)
    
    # Gráfico de bolhas
    st.markdown('<div class="sub-header">Matriz: Volume vs Faturamento vs Clientes</div>', unsafe_allow_html=True)
    
    fig10 = px.scatter(
        marca_analysis,
        x='Volume Total (HL)',
        y='Faturamento Total',
        size='Num Clientes',
        color='Ticket Médio',
        hover_name='Marca',
        title='Performance Multidimensional por Marca',
        labels={'Volume Total (HL)': 'Volume (HL)', 'Faturamento Total': 'Faturamento (R$)'},
        color_continuous_scale='Plasma',
        size_max=60
    )
    st.plotly_chart(fig10, use_container_width=True)
    
    # Seletor de marca
    st.markdown('<div class="sub-header">Análise Detalhada por Marca</div>', unsafe_allow_html=True)
    
    marca_selecionada = st.selectbox("Selecione uma marca:", marca_analysis['Marca'].tolist())
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Dados da marca selecionada
        marca_data = marca_analysis[marca_analysis['Marca'] == marca_selecionada].iloc[0]
        
        st.markdown("**Indicadores da Marca**")
        st.metric("Volume Total", f"{marca_data['Volume Total (HL)']:,.2f} HL")
        st.metric("Faturamento Total", f"R$ {marca_data['Faturamento Total']:,.2f}")
        st.metric("Número de Clientes", f"{int(marca_data['Num Clientes'])}")
        st.metric("Ticket Médio por HL", f"R$ {marca_data['Ticket Médio']:.2f}")
    
    with col2:
        # Meta da marca
        if marca_selecionada in metas['marca'].values:
            meta_marca_data = metas[metas['marca'] == marca_selecionada]
            meta_total = meta_marca_data['meta_marca'].iloc[0]
            num_clientes_meta = meta_marca_data['cliente_id'].nunique()
            
            st.markdown("**Metas 2025**")
            st.metric("Meta Estabelecida", f"{meta_total:,.0f} HL")
            st.metric("Clientes com Meta", f"{num_clientes_meta}")
            
            # Gap análise
            gap = meta_total - marca_data['Volume Total (HL)']
            gap_perc = (gap / marca_data['Volume Total (HL)'] * 100)
            

    
    # Top clientes da marca
    st.markdown(f'<div class="sub-header">Top 10 Clientes de {marca_selecionada}</div>', unsafe_allow_html=True)
    
    clientes_marca = fato[fato['marca'] == marca_selecionada].groupby('sk_cliente').agg({
        'volume_hl': 'sum',
        'valor': 'sum'
    }).reset_index().nlargest(10, 'volume_hl')
    
    fig11 = px.bar(
        clientes_marca,
        x='sk_cliente',
        y='volume_hl',
        title=f'Top 10 Clientes por Volume - {marca_selecionada}',
        labels={'sk_cliente': 'Cliente SK', 'volume_hl': 'Volume (HL)'},
        color='volume_hl',
        color_continuous_scale='Greens'
    )
    st.plotly_chart(fig11, use_container_width=True)

# ============================================================================
# PÁGINA 5: DADOS DETALHADOS
# ============================================================================
elif page == "Dados Detalhados":
    
    st.markdown('<div class="sub-header">Exploração de Dados Brutos</div>', unsafe_allow_html=True)
    
    tabela_selecionada = st.selectbox(
        "Selecione a tabela para visualizar:",
        ["Visão Consolidada", "Metas por Cliente", "Dimensão Cliente", "Dimensão Produto", "Fato Vendas"]
    )
    
    if tabela_selecionada == "Visão Consolidada":
        df = load_visao_consolidada()
        st.write(f"**Total de registros:** {len(df)}")
        st.dataframe(df, use_container_width=True)
    
    elif tabela_selecionada == "Metas por Cliente":
        df = load_metas_por_cliente()
        st.write(f"**Total de registros:** {len(df)}")
        st.dataframe(df, use_container_width=True)
    
    elif tabela_selecionada == "Dimensão Cliente":
        df = load_dim_cliente()
        st.write(f"**Total de clientes:** {len(df)}")
        st.dataframe(df, use_container_width=True)
    
    elif tabela_selecionada == "Dimensão Produto":
        df = load_dim_produto()
        st.write(f"**Total de produtos/marcas:** {len(df)}")
        st.dataframe(df, use_container_width=True)
    
    elif tabela_selecionada == "Fato Vendas":
        df = load_fato_vendas()
        st.write(f"**Total de transações:** {len(df)}")
        
        # Filtros
        col1, col2 = st.columns(2)
        with col1:
            marcas = ['Todas'] + sorted(df['marca'].unique().tolist())
            marca_filtro = st.selectbox("Filtrar por marca:", marcas)
        
        with col2:
            limit = st.slider("Número de registros:", 100, 10000, 1000, 100)
        
        if marca_filtro != 'Todas':
            df = df[df['marca'] == marca_filtro]
        
        st.dataframe(df.head(limit), use_container_width=True)
    
    # Download de dados
    st.markdown('<div class="sub-header">Exportar Dados</div>', unsafe_allow_html=True)
    
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"{tabela_selecionada.replace(' ', '_').lower()}_{datetime.now().strftime('%Y%m%d')}.csv",
        mime='text/csv'
    )

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #7f8c8d; font-size: 0.9rem;'>
    Dev= Matheus Rodrigues<br>
    Dados processados em {data}
</div>
""".format(data=datetime.now().strftime('%d/%m/%Y')), unsafe_allow_html=True)
