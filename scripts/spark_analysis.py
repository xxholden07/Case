# -*- coding: utf-8 -*-
"""
Análise de Vendas e Distribuição de Metas usando Apache Spark
Autor: Matheus Rodrigues
Data: 2025-10
"""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, sum, avg, count, max, min, when, lit, 
    year, month, last_day, add_months, months_between,
    coalesce, round, datediff, row_number, expr, date_format
)
from pyspark.sql.types import *
import requests
from datetime import datetime
from etl_logger import get_logger

# ============================================================================
# CONFIGURAÇÃO DO SPARK
# ============================================================================

def criar_spark_session():
    """Cria e configura a sessão Spark"""
    logger = get_logger()
    logger.info("Criando Spark Session...")
    
    spark = SparkSession.builder \
        .appName("Analise_Vendas_Metas") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("✓ Spark Session criada com sucesso")
    return spark


# ============================================================================
# 1. CARREGAMENTO DE DADOS
# ============================================================================

def carregar_dados(spark, sales_path, metas_path):
    """
    Carrega os arquivos de vendas e metas usando 100% PySpark nativo
    Converte automaticamente XLSX para CSV se necessário
    
    Args:
        spark: SparkSession
        sales_path: Caminho para o arquivo sales.xlsx
        metas_path: Caminho para o arquivo metas por marca.csv
    
    Returns:
        tuple: (df_sales, df_metas)
    """
    logger = get_logger()
    logger.step("CARREGAMENTO DE DADOS")
    
    import os
    sales_csv = sales_path.replace('.xlsx', '.csv')
    
    # Verificar se CSV existe
    if not os.path.exists(sales_csv):
        logger.error(f"Arquivo {sales_csv} não encontrado!")
        logger.info("Execute primeiro: .\\run_etl.ps1")
        raise FileNotFoundError(f"Arquivo {sales_csv} não encontrado. Execute run_etl.ps1 primeiro.")
    
    # Carregar CSV com PySpark (100% nativo, sem Pandas!)
    logger.info(f"Carregando dados de vendas: {sales_csv}")
    df_sales = spark.read.csv(sales_csv, header=True, inferSchema=True)
    registros_vendas = df_sales.count()
    logger.data_quality("df_sales (100% PySpark)", registros_vendas)
    
    # Carregar arquivo de metas (CSV com separador ;)
    logger.info(f"Carregando arquivo de metas: {metas_path}")
    df_metas = spark.read.csv(
        metas_path,
        header=True,
        inferSchema=True,
        sep=";"
    )
    registros_metas = df_metas.count()
    logger.data_quality("df_metas", registros_metas)
    
    return df_sales, df_metas


# ============================================================================
# 2. EXPLORAÇÃO E COMPREENSÃO DOS DADOS
# ============================================================================

def explorar_dados(df_sales, df_metas):
    """Analisa a estrutura e qualidade dos dados"""
    logger = get_logger()
    logger.step("EXPLORAÇÃO DOS DADOS")
    
    # Análise do DataFrame de Vendas
    logger.info("ESTRUTURA DO ARQUIVO DE VENDAS")
    df_sales.printSchema()
    
    logger.info("ESTATÍSTICAS DESCRITIVAS - VENDAS")
    df_sales.describe().show()
    
    logger.info("AMOSTRA DOS DADOS DE VENDAS (5 registros)")
    df_sales.show(5, truncate=False)
    
    logger.info("CONTAGEM DE REGISTROS POR COLUNA")
    for column in df_sales.columns:
        total = df_sales.count()
        non_null = df_sales.filter(col(column).isNotNull()).count()
        null_count = total - non_null
        null_pct = (null_count / total) * 100 if total > 0 else 0
        
        if null_count > 0:
            logger.warning(f"{column:30s}: {non_null:7d} não-nulos | {null_count:7d} nulos ({null_pct:.2f}%)")
        else:
            logger.info(f"{column:30s}: {non_null:7d} não-nulos | {null_count:7d} nulos ({null_pct:.2f}%)")
    
    # Análise do DataFrame de Metas
    logger.info("ESTRUTURA DO ARQUIVO DE METAS")
    df_metas.printSchema()
    
    logger.info("DADOS DE METAS")
    df_metas.show(truncate=False)


# ============================================================================
# 3. LIMPEZA E PREPARAÇÃO DOS DADOS
# ============================================================================

def limpar_dados(df_sales):
    """
    Realiza limpeza e preparação dos dados de vendas
    
    Args:
        df_sales: DataFrame de vendas bruto
    
    Returns:
        DataFrame: Dados limpos e preparados
    """
    logger = get_logger()
    logger.step("LIMPEZA E PREPARAÇÃO DOS DADOS")
    
    df_clean = df_sales
    
    # Converter data_doc para DateType (formato YYYY-MM-DD)
    if 'data_doc' in df_clean.columns:
        from pyspark.sql.functions import to_date
        df_clean = df_clean.withColumn('data_doc', to_date(col('data_doc'), 'yyyy-MM-dd'))
        logger.info("Coluna data_doc convertida para DateType")
    
    # Tratar valores ausentes em colunas críticas
    logger.info("Tratamento de valores ausentes:")
    
    # Para colunas numéricas, preencher com 0
    numeric_cols = [field.name for field in df_clean.schema.fields 
                   if isinstance(field.dataType, (IntegerType, LongType, DoubleType, FloatType, DecimalType))]
    
    for col_name in numeric_cols:
        null_count_before = df_clean.filter(col(col_name).isNull()).count()
        if null_count_before > 0:
            df_clean = df_clean.withColumn(col_name, coalesce(col(col_name), lit(0)))
            logger.warning(f"{col_name}: {null_count_before} valores nulos preenchidos com 0")
    
    # Para b2b_status, assumir False se nulo
    if 'b2b_status' in df_clean.columns:
        null_count_before = df_clean.filter(col('b2b_status').isNull()).count()
        if null_count_before > 0:
            df_clean = df_clean.withColumn('b2b_status', coalesce(col('b2b_status'), lit(False)))
            logger.warning(f"b2b_status: {null_count_before} valores nulos preenchidos com False")
    
    # Para campos de texto, preencher com "Não informado"
    string_cols = [field.name for field in df_clean.schema.fields 
                  if isinstance(field.dataType, StringType)]
    
    for col_name in string_cols:
        null_count_before = df_clean.filter(col(col_name).isNull()).count()
        if null_count_before > 0:
            df_clean = df_clean.withColumn(col_name, coalesce(col(col_name), lit("Não informado")))
            logger.warning(f"{col_name}: {null_count_before} valores nulos preenchidos")
    
    # Adicionar colunas derivadas úteis
    if 'data_pedido' in df_clean.columns or 'data' in df_clean.columns:
        data_col = 'data_pedido' if 'data_pedido' in df_clean.columns else 'data'
        df_clean = df_clean \
            .withColumn('ano', year(col(data_col))) \
            .withColumn('mes', month(col(data_col))) \
            .withColumn('ano_mes', expr(f"date_format({data_col}, 'yyyy-MM')"))
        logger.info("Colunas derivadas criadas: ano, mes, ano_mes")
    
    total_registros = df_clean.count()
    logger.info(f"✓ Limpeza concluída: {total_registros} registros preparados")
    
    return df_clean


# ============================================================================
# 4. MODELAGEM DIMENSIONAL (STAR SCHEMA)
# ============================================================================

def criar_modelo_dimensional(df_sales):
    """
    Cria modelo dimensional (star schema) a partir dos dados de vendas
    
    Args:
        df_sales: DataFrame de vendas limpo
    
    Returns:
        dict: Dicionário com fato e dimensões
    """
    logger = get_logger()
    logger.step("CRIAÇÃO DO MODELO DIMENSIONAL (STAR SCHEMA)")
    
    modelo = {}
    
    # Identificar coluna de data
    data_col = None
    for c in df_sales.columns:
        if 'data' in c.lower():
            data_col = c
            break
    
    # DIMENSÃO CLIENTE
    logger.info("Criando dim_cliente...")
    dim_cliente = df_sales.select(
        col('Código do Cliente').alias('cliente_id'),
        col('CEP_cliente').alias('cep')
    ).distinct()
    
    dim_cliente = dim_cliente.withColumn(
        'sk_cliente',
        row_number().over(Window.orderBy('cliente_id'))
    ).select('sk_cliente', 'cliente_id', 'cep')
    
    modelo['dim_cliente'] = dim_cliente
    logger.data_quality("dim_cliente", dim_cliente.count())
    
    # DIMENSÃO PRODUTO/MARCA
    logger.info("Criando dim_produto...")
    if 'brand_desc' in df_sales.columns:
        dim_produto = df_sales.select(
            col('brand_desc').alias('marca')
        ).distinct()
        
        dim_produto = dim_produto.withColumn(
            'sk_produto',
            row_number().over(Window.orderBy('marca'))
        ).select('sk_produto', 'marca')
        
        modelo['dim_produto'] = dim_produto
        logger.data_quality("dim_produto", dim_produto.count())
    
    # DIMENSÃO TEMPO
    if data_col and ('ano' in df_sales.columns):
        logger.info("Criando dim_tempo...")
        dim_tempo = df_sales.select(
            col(data_col).alias('data'),
            col('ano'),
            col('mes'),
            col('ano_mes')
        ).distinct()
        
        dim_tempo = dim_tempo.withColumn(
            'sk_tempo',
            row_number().over(Window.orderBy('data'))
        ).select('sk_tempo', 'data', 'ano', 'mes', 'ano_mes')
        
        modelo['dim_tempo'] = dim_tempo
        logger.data_quality("dim_tempo", dim_tempo.count())
    
    # FATO VENDAS
    logger.info("Criando fato_vendas...")
    fato = df_sales
    
    # Join com dimensões
    fato = fato.join(
        dim_cliente.select('sk_cliente', 'cliente_id'),
        fato['Código do Cliente'] == dim_cliente['cliente_id'],
        'left'
    )
    
    if 'dim_produto' in modelo:
        fato = fato.withColumn('marca', col('brand_desc'))
        fato = fato.join(
            modelo['dim_produto'].select('sk_produto', 'marca'),
            'marca',
            'left'
        )
    
    if data_col and 'dim_tempo' in modelo:
        fato = fato.join(
            modelo['dim_tempo'].select('sk_tempo', 'data'),
            fato[data_col] == modelo['dim_tempo']['data'],
            'left'
        )
    
    # Selecionar colunas do fato
    fato_cols = ['sk_cliente']
    if 'dim_tempo' in modelo:
        fato_cols.append('sk_tempo')
    if 'dim_produto' in modelo:
        fato_cols.append('sk_produto')
    
    # Adicionar métricas (usar nomes reais das colunas)
    fato_cols.extend(['volume_hl', 'valor', 'b2b_status'])
    
    fato_vendas = fato.select(*fato_cols)
    modelo['fato_vendas'] = fato_vendas
    logger.data_quality("fato_vendas", fato_vendas.count())
    
    logger.info("✓ Modelo dimensional criado com sucesso!")
    
    return modelo


# ============================================================================
# 5. VISÃO CONSOLIDADA POR CLIENTE
# ============================================================================

def criar_visao_consolidada(df_sales):
    """
    Cria visão mensal consolidada por cliente com métricas rolling
    
    Métricas calculadas:
    - Volume médio últimos 3 meses (a partir da última compra)
    - Volume médio últimos 6 meses (a partir da última compra)
    - Share B2B últimos 3 meses (a partir da última compra)
    - Share B2B últimos 6 meses (a partir da última compra)
    
    Args:
        df_sales: DataFrame de vendas limpo
    
    Returns:
        DataFrame: Visão consolidada por cliente
    """
    logger = get_logger()
    logger.step("CRIAÇÃO DA VISÃO CONSOLIDADA POR CLIENTE")
    
    # Usar nomes reais das colunas
    data_col = 'data_doc'
    volume_col = 'volume_hl'
    faturamento_col = 'valor'
    cliente_col = 'Código do Cliente'
    
    # 1. Identificar a última compra de cada cliente
    logger.info("Identificando última compra de cada cliente...")
    ultima_compra = df_sales.groupBy(cliente_col).agg(
        max(col(data_col)).alias('data_ultima_compra')
    )
    
    # 2. Preparar dados com informação de última compra
    df_com_ultima = df_sales.join(ultima_compra, cliente_col)
    
    # 3. Calcular diferença em meses entre cada transação e a última compra
    # Valores negativos = transações no passado | 0 = última compra | Positivos = futuro (não existem)
    df_com_ultima = df_com_ultima.withColumn(
        'meses_da_ultima',
        months_between(col('data_ultima_compra'), col(data_col))
    )
    
    # 3.5. Criar coluna ano_mes para agregação
    df_com_ultima = df_com_ultima.withColumn(
        'ano_mes',
        date_format(col(data_col), 'yyyy-MM')
    )
    
    # 4. Agregar por cliente e mês
    logger.info("Agregando dados por cliente e mês...")
    df_mensal = df_com_ultima.groupBy(
        cliente_col,
        'ano_mes',
        'data_ultima_compra',
        'meses_da_ultima'
    ).agg(
        sum(volume_col).alias('volume_mensal'),
        sum(faturamento_col).alias('faturamento_total_mensal'),
        sum(
            when(col('b2b_status') == True, col(faturamento_col))
            .otherwise(0)
        ).alias('faturamento_b2b_mensal')
    )
    
    # 5. Para cada cliente, calcular métricas rolling a partir da última compra
    logger.info("Calculando métricas rolling (3 e 6 meses)...")
    
    # Calcular métricas dos últimos 3 meses (a partir da última compra)
    # meses_da_ultima = meses entre última compra e cada transação
    # Valores <= 3 = transações nos últimos 3 meses
    df_mensal = df_mensal.withColumn(
        'volume_medio_3m',
        coalesce(
            avg(
                when(col('meses_da_ultima') <= 3, col('volume_mensal'))
            ).over(Window.partitionBy(cliente_col)),
            lit(0.0)
        )
    )
    
    df_mensal = df_mensal.withColumn(
        'faturamento_3m_total',
        coalesce(
            sum(
                when(col('meses_da_ultima') <= 3, col('faturamento_total_mensal'))
                .otherwise(0)
            ).over(Window.partitionBy(cliente_col)),
            lit(0.0)
        )
    )
    
    df_mensal = df_mensal.withColumn(
        'faturamento_3m_b2b',
        coalesce(
            sum(
                when(col('meses_da_ultima') <= 3, col('faturamento_b2b_mensal'))
                .otherwise(0)
            ).over(Window.partitionBy(cliente_col)),
            lit(0.0)
        )
    )
    
    df_mensal = df_mensal.withColumn(
        'share_b2b_3m',
        when(col('faturamento_3m_total') > 0,
             round((col('faturamento_3m_b2b') / col('faturamento_3m_total')) * 100, 2)
        ).otherwise(0)
    )
    
    # Calcular métricas dos últimos 6 meses (a partir da última compra)
    df_mensal = df_mensal.withColumn(
        'volume_medio_6m',
        coalesce(
            avg(
                when(col('meses_da_ultima') <= 6, col('volume_mensal'))
            ).over(Window.partitionBy(cliente_col)),
            lit(0.0)
        )
    )
    
    df_mensal = df_mensal.withColumn(
        'faturamento_6m_total',
        coalesce(
            sum(
                when(col('meses_da_ultima') <= 6, col('faturamento_total_mensal'))
                .otherwise(0)
            ).over(Window.partitionBy(cliente_col)),
            lit(0.0)
        )
    )
    
    df_mensal = df_mensal.withColumn(
        'faturamento_6m_b2b',
        coalesce(
            sum(
                when(col('meses_da_ultima') <= 6, col('faturamento_b2b_mensal'))
                .otherwise(0)
            ).over(Window.partitionBy(cliente_col)),
            lit(0.0)
        )
    )
    
    df_mensal = df_mensal.withColumn(
        'share_b2b_6m',
        when(col('faturamento_6m_total') > 0,
             round((col('faturamento_6m_b2b') / col('faturamento_6m_total')) * 100, 2)
        ).otherwise(0)
    )
    
    # 6. Consolidar por cliente (pegar valores únicos, pois são iguais para todos os meses do mesmo cliente)
    logger.info("Consolidando visão final por cliente...")
    visao_consolidada = df_mensal.select(
        col(cliente_col).alias('cliente_id'),
        'data_ultima_compra',
        'volume_medio_3m',
        'volume_medio_6m',
        'share_b2b_3m',
        'share_b2b_6m'
    ).distinct()
    
    total_clientes = visao_consolidada.count()
    logger.info(f"✓ Visão consolidada criada: {total_clientes} clientes")
    
    logger.info("AMOSTRA DA VISÃO CONSOLIDADA")
    visao_consolidada.show(10, truncate=False)
    
    return visao_consolidada


# ============================================================================
# 6. DISTRIBUIÇÃO DE METAS POR CLIENTE
# ============================================================================

def distribuir_metas_por_cliente(df_sales, df_metas, ano=2025):
    """
    Distribui metas de marca para o nível de cliente proporcionalmente
    
    Args:
        df_sales: DataFrame de vendas
        df_metas: DataFrame de metas por marca
        ano: Ano das metas a distribuir
    
    Returns:
        DataFrame: Metas distribuídas por cliente e marca
    """
    logger = get_logger()
    logger.step(f"DISTRIBUIÇÃO DE METAS POR CLIENTE - ANO {ano}")
    
    volume_col = 'volume_hl'
    cliente_col = 'Código do Cliente'
    marca_col = 'brand_desc'
    
    # 1. Filtrar metas do ano especificado
    logger.info(f"Filtrando metas do ano {ano}...")
    df_metas_ano = df_metas.filter(col('Ano') == ano)
    
    total_metas = df_metas_ano.count()
    logger.data_quality(f"metas_{ano}", total_metas)
    
    if total_metas == 0:
        logger.query_result(f"QUERY VAZIA: Nenhuma meta encontrada para o ano {ano}")
    
    df_metas_ano.show()
    
    # 2. Calcular volume total por cliente e marca
    logger.info("Calculando volume por cliente e marca...")
    volume_cliente_marca = df_sales.groupBy(cliente_col, marca_col).agg(
        sum(volume_col).alias('volume_cliente_marca')
    )
    
    # 3. Calcular volume total por marca (todos os clientes)
    logger.info("Calculando volume total por marca...")
    volume_total_marca = df_sales.groupBy(marca_col).agg(
        sum(volume_col).alias('volume_total_marca')
    )
    
    # 4. Calcular percentual de cada cliente dentro da marca
    logger.info("Calculando participação percentual de cada cliente...")
    df_participacao = volume_cliente_marca.join(
        volume_total_marca,
        marca_col
    )
    
    df_participacao = df_participacao.withColumn(
        'percentual_cliente',
        (col('volume_cliente_marca') / col('volume_total_marca')) * 100
    )
    
    # 5. Juntar com metas e calcular meta do cliente
    logger.info("Distribuindo metas proporcionalmente...")
    df_metas_cliente = df_participacao.join(
        df_metas_ano.select(
            col('Marca').alias(marca_col),
            col('Meta').alias('meta_marca'),
            col('Ano').alias('ano_meta')
        ),
        marca_col,
        'inner'
    )
    
    df_metas_cliente = df_metas_cliente.withColumn(
        'meta_cliente',
        round((col('percentual_cliente') / 100) * col('meta_marca'), 2)
    )
    
    # 6. Selecionar colunas finais
    df_metas_cliente = df_metas_cliente.select(
        col(cliente_col).alias('cliente_id'),
        col(marca_col).alias('marca'),
        'ano_meta',
        'volume_cliente_marca',
        'volume_total_marca',
        round(col('percentual_cliente'), 2).alias('percentual_participacao'),
        'meta_marca',
        'meta_cliente'
    ).orderBy('marca', col('meta_cliente').desc())
    
    total_metas_distribuidas = df_metas_cliente.count()
    logger.info(f"✓ Metas distribuídas: {total_metas_distribuidas} registros (cliente x marca)")
    
    logger.info("AMOSTRA DAS METAS DISTRIBUÍDAS")
    df_metas_cliente.show(20, truncate=False)
    
    # Validação: soma das metas dos clientes deve ser igual à meta da marca
    logger.info("VALIDAÇÃO: Comparação entre Meta da Marca e Soma das Metas dos Clientes")
    validacao = df_metas_cliente.groupBy('marca', 'meta_marca').agg(
        round(sum('meta_cliente'), 2).alias('soma_metas_clientes')
    )
    
    validacao = validacao.withColumn(
        'diferenca',
        round(col('meta_marca') - col('soma_metas_clientes'), 2)
    )
    
    validacao.show(truncate=False)
    
    # Log de validação de diferenças
    diferencas = validacao.filter(col('diferenca') != 0).count()
    if diferencas > 0:
        logger.warning(f"ATENÇÃO: {diferencas} marcas com diferença entre meta e soma das metas dos clientes")
    else:
        logger.validation("Soma das metas dos clientes = Meta da marca", True)
    
    return df_metas_cliente


# ============================================================================
# 7. ENRIQUECIMENTO COM API DE CEP
# ============================================================================

def consultar_cep(cep):
    """
    Consulta API ViaCEP para obter cidade e estado
    
    Args:
        cep: CEP a consultar (string)
    
    Returns:
        tuple: (cidade, estado) ou (None, None) em caso de erro
    """
    try:
        # Remover caracteres não numéricos
        cep_limpo = ''.join(filter(str.isdigit, str(cep)))
        
        if len(cep_limpo) != 8:
            return None, None
        
        # Consultar API ViaCEP
        url = f"https://viacep.com.br/ws/{cep_limpo}/json/"
        response = requests.get(url, timeout=5)
        
        if response.status_code == 200:
            dados = response.json()
            if 'erro' not in dados:
                return dados.get('localidade'), dados.get('uf')
        
        return None, None
    
    except Exception as e:
        print(f"  ⚠ Erro ao consultar CEP {cep}: {str(e)}")
        return None, None


def enriquecer_dados_cep(spark, df_com_clientes):
    """
    Enriquece DataFrame com dados de cidade e estado via API de CEP
    
    Args:
        spark: SparkSession
        df_com_clientes: DataFrame com coluna 'cep' ou 'cliente_id'
    
    Returns:
        DataFrame: DataFrame enriquecido com cidade e estado
    """
    logger = get_logger()
    logger.step("ENRIQUECIMENTO DE DADOS VIA API DE CEP")
    
    # Verificar se tem coluna 'cep', se não tiver, assumir que é 'cliente_id'
    if 'cep' not in df_com_clientes.columns:
        logger.warning("Coluna 'cep' não encontrada, assumindo que DataFrame tem 'cliente_id'")
        # Este DataFrame provavelmente só tem cliente_id, precisamos adicionar CEP
        # Retornar sem enriquecimento por enquanto
        df_com_clientes = df_com_clientes.withColumn('cidade', lit('Não consultado'))
        df_com_clientes = df_com_clientes.withColumn('estado', lit('Não consultado'))
        logger.warning("Enriquecimento CEP pulado (coluna 'cep' não disponível)")
        return df_com_clientes
    
    # 1. Extrair CEPs únicos usando Pandas (evita problemas de conexão Spark durante API calls)
    logger.info("Extraindo CEPs únicos para consulta...")
    import pandas as pd
    ceps_pandas = df_com_clientes.select('cep').distinct().toPandas()
    total_ceps = len(ceps_pandas)
    logger.data_quality("ceps_unicos", total_ceps)
    
    # 2. Consultar API para cada CEP
    logger.info("Consultando API ViaCEP...")
    dados_cep = []
    
    for i, cep in enumerate(ceps_pandas['cep'], 1):
        if cep and cep != "Não informado":
            cidade, estado = consultar_cep(cep)
            dados_cep.append({
                'cep': cep,
                'cidade': cidade if cidade else 'Não encontrado',
                'estado': estado if estado else 'Não encontrado'
            })
            
            if i % 10 == 0:
                logger.info(f"  Progresso: {i}/{total_ceps} CEPs consultados")
        else:
            dados_cep.append({
                'cep': cep,
                'cidade': 'Não informado',
                'estado': 'Não informado'
            })
    
    logger.info(f"{len(dados_cep)} consultas de CEP concluídas")
    
    # 3. Criar DataFrame PySpark diretamente com schema explícito (evita problemas de compatibilidade Pandas 2.x)
    logger.info("Criando DataFrame com dados enriquecidos...")
    
    schema_cep = StructType([
        StructField("cep", IntegerType(), True),
        StructField("cidade", StringType(), True),
        StructField("estado", StringType(), True)
    ])
    
    df_cep_enriquecido = spark.createDataFrame(dados_cep, schema=schema_cep)
    
    # 4. Juntar com DataFrame original
    logger.info("Incorporando dados ao DataFrame original...")
    df_enriquecido = df_com_clientes.join(
        df_cep_enriquecido,
        'cep',
        'left'
    )
    
    logger.info("✓ Enriquecimento concluído!")
    
    if 'cep' in df_enriquecido.columns and 'cidade' in df_enriquecido.columns:
        logger.info("AMOSTRA DOS DADOS ENRIQUECIDOS")
        df_enriquecido.select('cep', 'cidade', 'estado') \
            .distinct() \
            .show(10, truncate=False)
    
    return df_enriquecido


# ============================================================================
# 8. EXPORTAÇÃO DE RESULTADOS
# ============================================================================

def exportar_resultados(df_dict, output_dir):
    """
    Exporta DataFrames para arquivos CSV e Parquet
    
    Args:
        df_dict: Dicionário com nome -> DataFrame
        output_dir: Diretório de saída
    """
    logger = get_logger()
    logger.step("EXPORTAÇÃO DE RESULTADOS")
    
    import os
    
    # Criar diretórios se não existirem
    csv_dir = os.path.join(output_dir, 'csv')
    parquet_dir = os.path.join(output_dir, 'parquet')
    
    os.makedirs(csv_dir, exist_ok=True)
    os.makedirs(parquet_dir, exist_ok=True)
    
    for nome, df in df_dict.items():
        logger.info(f"Exportando {nome}...")
        
        try:
            # Tentar exportar como CSV usando Spark (pode falhar no Windows sem Hadoop)
            csv_path = os.path.join(csv_dir, nome)
            df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(csv_path)
            logger.info(f"CSV (Spark): {csv_path}")
        except Exception as e:
            # Fallback: usar Pandas apenas para salvar (processamento foi 100% Spark)
            logger.warning(f"Spark write falhou (falta winutils no Windows), usando Pandas para salvar...")
            import pandas as pd
            csv_file = os.path.join(csv_dir, f"{nome}.csv")
            df.toPandas().to_csv(csv_file, index=False)
            logger.info(f"CSV (via Pandas): {csv_file}")
        
        try:
            # Parquet (formato otimizado)
            parquet_path = os.path.join(parquet_dir, nome)
            df.write.mode('overwrite').parquet(parquet_path)
            logger.info(f"Parquet: {parquet_path}")
        except Exception as e:
            # Fallback para Parquet também
            logger.warning(f"Spark parquet falhou, usando Pandas...")
            import pandas as pd
            parquet_file = os.path.join(parquet_dir, f"{nome}.parquet")
            df.toPandas().to_parquet(parquet_file, index=False)
            logger.info(f"Parquet (via Pandas): {parquet_file}")
    
    logger.info("✓ Todas as exportações concluídas!")


# ============================================================================
# 9. FUNÇÃO PRINCIPAL
# ============================================================================

def main():
    """Função principal que orquestra todo o processamento"""
    
    logger = get_logger()
    logger.step("ANÁLISE DE VENDAS E DISTRIBUIÇÃO DE METAS COM APACHE SPARK")
    logger.info(f"Data de execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Caminhos dos arquivos
    BASE_DIR = r"C:\Users\matheus.rodrigues\Downloads\Case"
    SALES_PATH = rf"{BASE_DIR}\input\sales.csv"
    METAS_PATH = rf"{BASE_DIR}\input\metas por marca.csv"
    OUTPUT_DIR = rf"{BASE_DIR}\output"
    
    try:
        # 1. Criar sessão Spark
        spark = criar_spark_session()
        
        # 2. Carregar dados
        df_sales, df_metas = carregar_dados(spark, SALES_PATH, METAS_PATH)
        
        # 3. Explorar dados
        explorar_dados(df_sales, df_metas)
        
        # 4. Limpar e preparar dados
        df_sales_clean = limpar_dados(df_sales)
        
        # 5. Criar modelo dimensional
        modelo = criar_modelo_dimensional(df_sales_clean)
        
        # 6. Adicionar colunas de cidade e estado (serão preenchidas por script separado)
        logger.info("Preparando dim_cliente para enriquecimento de CEP...")
        modelo['dim_cliente'] = modelo['dim_cliente'] \
            .withColumn('cidade', lit(None).cast(StringType())) \
            .withColumn('estado', lit(None).cast(StringType()))
        
        # 7. Criar visão consolidada por cliente
        visao_consolidada = criar_visao_consolidada(df_sales_clean)
        
        # 8. Adicionar colunas vazias de cidade/estado à visão consolidada
        visao_consolidada_enriquecida = visao_consolidada \
            .withColumn('cidade', lit(None).cast(StringType())) \
            .withColumn('estado', lit(None).cast(StringType()))
        
        # 9. Distribuir metas por cliente
        metas_cliente = distribuir_metas_por_cliente(df_sales_clean, df_metas, ano=2025)
        
        # 10. Adicionar colunas vazias de cidade/estado às metas
        metas_cliente_enriquecidas = metas_cliente \
            .withColumn('cidade', lit(None).cast(StringType())) \
            .withColumn('estado', lit(None).cast(StringType()))
        
        # 11. Exportar resultados
        resultados = {
            'visao_consolidada': visao_consolidada_enriquecida,
            'metas_por_cliente': metas_cliente_enriquecidas,
            'dim_cliente': modelo['dim_cliente'],
            'fato_vendas': modelo['fato_vendas']
        }
        
        if 'dim_produto' in modelo:
            resultados['dim_produto'] = modelo['dim_produto']
        
        if 'dim_tempo' in modelo:
            resultados['dim_tempo'] = modelo['dim_tempo']
        
        exportar_resultados(resultados, OUTPUT_DIR)
        
        logger.step("PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
        logger.info(f"Resultados salvos em: {OUTPUT_DIR}")
        logger.info("\nArquivos gerados:")
        logger.info("  • visao_consolidada: Métricas consolidadas por cliente")
        logger.info("  • metas_por_cliente: Metas distribuídas por cliente e marca")
        logger.info("  • dim_cliente: Dimensão de clientes")
        logger.info("  • dim_produto: Dimensão de produtos/marcas")
        logger.info("  • dim_tempo: Dimensão de tempo")
        logger.info("  • fato_vendas: Fato de vendas (star schema)")
        
        logger.finalize()
        
    except Exception as e:
        logger.exception(f"ERRO DURANTE O PROCESSAMENTO: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Encerrar sessão Spark
        if 'spark' in locals():
            spark.stop()
            logger.info("Sessão Spark encerrada")


if __name__ == "__main__":
    main()
