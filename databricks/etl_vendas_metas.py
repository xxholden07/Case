# Databricks notebook source
# MAGIC %md
# MAGIC # ETL - Análise de Vendas e Distribuição de Metas
# MAGIC 
# MAGIC **Autor:** Matheus Rodrigues  
# MAGIC **Data:** Outubro 2025
# MAGIC 
# MAGIC ## Objetivos:
# MAGIC 1. Carregar dados de vendas e metas
# MAGIC 2. Processar e limpar dados usando Spark
# MAGIC 3. Criar modelo dimensional (Star Schema)
# MAGIC 4. Distribuir metas por cliente
# MAGIC 5. Exportar resultados

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração Inicial

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, sum, avg, count, max, min, when, lit, 
    year, month, last_day, add_months, months_between,
    coalesce, round, datediff, row_number, expr, date_format
)
from pyspark.sql.types import *
from datetime import datetime

# Configurações
print(f"Iniciando processamento ETL - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Definição de Parâmetros

# COMMAND ----------

# Widgets para parametrização (podem ser definidos no Job)
# Usando /FileStore que está disponível mesmo com DBFS público desabilitado
dbutils.widgets.text("input_path", "/FileStore/tables/heineken", "Caminho dos arquivos de entrada")
dbutils.widgets.text("output_path", "/FileStore/tables/heineken/output", "Caminho de saída")
dbutils.widgets.text("ano_meta", "2025", "Ano para distribuição de metas")

# Obter valores
INPUT_PATH = dbutils.widgets.get("input_path")
OUTPUT_PATH = dbutils.widgets.get("output_path")
ANO_META = int(dbutils.widgets.get("ano_meta"))

print(f"Caminho de entrada: {INPUT_PATH}")
print(f"Caminho de saída: {OUTPUT_PATH}")
print(f"Ano de metas: {ANO_META}")

# Verificar se os arquivos existem
try:
    files = dbutils.fs.ls(INPUT_PATH)
    print(f"\nArquivos encontrados em {INPUT_PATH}:")
    for file in files:
        print(f"  - {file.name} ({file.size} bytes)")
    print("\nSUCESSO! Arquivos acessíveis.")
except Exception as e:
    print(f"\nERRO: Não foi possível acessar {INPUT_PATH}")
    print(f"Erro: {str(e)}")
    print("\n" + "="*80)
    print("SOLUÇÃO: Faça upload dos arquivos via interface:")
    print("1. Menu lateral esquerdo > DATA")
    print("2. Clique em 'Create' ou '+' > Upload File")
    print("3. Navegue até: /FileStore/tables/")
    print("4. Crie a pasta 'heineken'")
    print("5. Faça upload de:")
    print("   - sales.csv")
    print("   - metas por marca.csv")
    print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Carregamento de Dados

# COMMAND ----------

def carregar_dados(sales_path, metas_path):
    """Carrega os arquivos de vendas e metas"""
    
    print("Carregando arquivo de vendas...")
    df_sales = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(sales_path)
    
    print(f"   {df_sales.count():,} registros de vendas carregados")
    
    print("Carregando arquivo de metas...")
    df_metas = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(metas_path)
    
    print(f"   {df_metas.count():,} registros de metas carregados")
    
    return df_sales, df_metas

# Carregar dados
SALES_PATH = f"{INPUT_PATH}/sales.csv"
METAS_PATH = f"{INPUT_PATH}/metas por marca.csv"

df_sales, df_metas = carregar_dados(SALES_PATH, METAS_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Exploração Inicial dos Dados

# COMMAND ----------

print("RESUMO DOS DADOS DE VENDAS")
print("=" * 80)
df_sales.printSchema()
print(f"\nTotal de registros: {df_sales.count():,}")
print(f"Colunas: {len(df_sales.columns)}")

display(df_sales.limit(10))

# COMMAND ----------

print("RESUMO DOS DADOS DE METAS")
print("=" * 80)
df_metas.printSchema()
print(f"\nTotal de registros: {df_metas.count():,}")
print(f"Colunas: {len(df_metas.columns)}")

display(df_metas.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Limpeza e Preparação dos Dados

# COMMAND ----------

def limpar_dados(df_sales):
    """Limpa e padroniza os dados de vendas"""
    
    print("Limpando dados de vendas...")
    
    # Remover duplicatas
    inicial = df_sales.count()
    df_clean = df_sales.dropDuplicates()
    removidos = inicial - df_clean.count()
    print(f"   {removidos:,} duplicatas removidas")
    
    # Remover nulos em campos essenciais
    df_clean = df_clean.filter(
        col("client_id").isNotNull() & 
        col("date").isNotNull() & 
        col("volume_hl").isNotNull()
    )
    
    # Padronizar tipos
    df_clean = df_clean.withColumn("volume_hl", col("volume_hl").cast("double")) \
                       .withColumn("b2b_status", col("b2b_status").cast("integer"))
    
    # Adicionar coluna de valor (se não existir)
    if "valor" not in df_clean.columns:
        df_clean = df_clean.withColumn("valor", col("volume_hl") * 100)  # Valor estimado
    
    print(f"   Dados limpos: {df_clean.count():,} registros")
    
    return df_clean

df_sales_clean = limpar_dados(df_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Criação do Modelo Dimensional (Star Schema)

# COMMAND ----------

def criar_modelo_dimensional(df_sales):
    """Cria o modelo dimensional completo"""
    
    print("Criando modelo dimensional...")
    
    # ===== DIMENSÃO CLIENTE =====
    print("   Criando dim_cliente...")
    dim_cliente = df_sales.select("client_id", "cep") \
        .dropDuplicates() \
        .withColumn("sk_cliente", row_number().over(Window.orderBy("client_id"))) \
        .select("sk_cliente", 
                col("client_id").alias("cliente_id"),
                "cep")
    
    print(f"      {dim_cliente.count():,} clientes únicos")
    
    # ===== DIMENSÃO PRODUTO =====
    print("   Criando dim_produto...")
    dim_produto = df_sales.select("brand") \
        .dropDuplicates() \
        .filter(col("brand").isNotNull()) \
        .withColumn("sk_produto", row_number().over(Window.orderBy("brand"))) \
        .select("sk_produto", 
                col("brand").alias("marca"))
    
    print(f"      {dim_produto.count():,} produtos/marcas únicos")
    
    # ===== DIMENSÃO TEMPO =====
    print("   Criando dim_tempo...")
    dim_tempo = df_sales.select("date") \
        .dropDuplicates() \
        .withColumn("sk_tempo", row_number().over(Window.orderBy("date"))) \
        .withColumn("ano", year("date")) \
        .withColumn("mes", month("date")) \
        .withColumn("trimestre", expr("quarter(date)")) \
        .select("sk_tempo", "date", "ano", "mes", "trimestre")
    
    print(f"      {dim_tempo.count():,} datas únicas")
    
    # ===== FATO VENDAS =====
    print("   Criando fato_vendas...")
    fato_vendas = df_sales \
        .join(dim_cliente, df_sales.client_id == dim_cliente.cliente_id, "left") \
        .join(dim_produto, df_sales.brand == dim_produto.marca, "left") \
        .join(dim_tempo, df_sales.date == dim_tempo.date, "left") \
        .select(
            "sk_cliente",
            "sk_produto",
            "sk_tempo",
            col("date").alias("data"),
            "volume_hl",
            "valor",
            "b2b_status"
        )
    
    print(f"      {fato_vendas.count():,} transações no fato")
    
    return {
        'dim_cliente': dim_cliente,
        'dim_produto': dim_produto,
        'dim_tempo': dim_tempo,
        'fato_vendas': fato_vendas
    }

modelo = criar_modelo_dimensional(df_sales_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Criação da Visão Consolidada por Cliente

# COMMAND ----------

def criar_visao_consolidada(df_sales):
    """Cria visão consolidada com métricas por cliente"""
    
    print("Criando visão consolidada por cliente...")
    
    # Calcular data de referência (data mais recente nos dados)
    data_referencia = df_sales.select(max("date")).collect()[0][0]
    print(f"   Data de referência: {data_referencia}")
    
    # Calcular limites de tempo
    data_3m = add_months(lit(data_referencia), -3)
    data_6m = add_months(lit(data_referencia), -6)
    
    # Visão consolidada por cliente
    visao = df_sales.groupBy("client_id").agg(
        max("date").alias("data_ultima_compra"),
        
        # Volume médio últimos 3 meses
        avg(when(col("date") >= data_3m, col("volume_hl"))).alias("volume_medio_3m"),
        
        # Volume médio últimos 6 meses
        avg(when(col("date") >= data_6m, col("volume_hl"))).alias("volume_medio_6m"),
        
        # Share B2B últimos 3 meses
        (sum(when((col("date") >= data_3m) & (col("b2b_status") == 1), col("volume_hl"))) / 
         sum(when(col("date") >= data_3m, col("volume_hl"))) * 100).alias("share_b2b_3m"),
        
        # Share B2B últimos 6 meses
        (sum(when((col("date") >= data_6m) & (col("b2b_status") == 1), col("volume_hl"))) / 
         sum(when(col("date") >= data_6m, col("volume_hl"))) * 100).alias("share_b2b_6m")
    ).withColumnRenamed("client_id", "cliente_id")
    
    print(f"   Visão consolidada criada para {visao.count():,} clientes")
    
    return visao

visao_consolidada = criar_visao_consolidada(df_sales_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Distribuição de Metas por Cliente

# COMMAND ----------

def distribuir_metas_por_cliente(df_sales, df_metas, ano=2025):
    """Distribui metas por marca proporcionalmente ao volume de cada cliente"""
    
    print(f"Distribuindo metas por cliente para o ano {ano}...")
    
    # Calcular volume total por cliente e marca
    volume_cliente_marca = df_sales.groupBy("client_id", "brand").agg(
        sum("volume_hl").alias("volume_total")
    )
    
    # Calcular volume total por marca
    volume_total_marca = df_sales.groupBy("brand").agg(
        sum("volume_hl").alias("volume_marca_total")
    )
    
    # Juntar para calcular percentual de participação
    participacao = volume_cliente_marca.join(
        volume_total_marca, 
        "brand"
    ).withColumn(
        "percentual_participacao",
        round((col("volume_total") / col("volume_marca_total")) * 100, 4)
    )
    
    # Juntar com metas
    metas_distribuidas = participacao.join(
        df_metas.select(
            col("marca").alias("brand"),
            col("meta").alias("meta_marca")
        ),
        "brand"
    ).withColumn(
        "meta_cliente",
        round(col("meta_marca") * col("percentual_participacao") / 100, 2)
    ).select(
        col("client_id").alias("cliente_id"),
        col("brand").alias("marca"),
        "volume_total",
        "percentual_participacao",
        "meta_marca",
        "meta_cliente"
    )
    
    print(f"   Metas distribuídas para {metas_distribuidas.count():,} combinações cliente-marca")
    
    # Validação
    print("\n   Validação das metas:")
    validacao = metas_distribuidas.groupBy("marca").agg(
        sum("meta_cliente").alias("soma_metas_clientes"),
        max("meta_marca").alias("meta_original")
    ).withColumn(
        "diferenca",
        round(col("meta_original") - col("soma_metas_clientes"), 2)
    )
    
    validacao.show()
    
    return metas_distribuidas

metas_cliente = distribuir_metas_por_cliente(df_sales_clean, df_metas, ANO_META)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Exportação dos Resultados

# COMMAND ----------

def exportar_para_delta(df, path, nome):
    """Exporta DataFrame para formato Delta Lake"""
    print(f"Exportando {nome}...")
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(path)
    
    print(f"   {nome} salvo em {path}")

# Exportar todas as tabelas
print("Exportando resultados para Delta Lake...")
print("=" * 80)

exportar_para_delta(visao_consolidada, f"{OUTPUT_PATH}/visao_consolidada", "Visão Consolidada")
exportar_para_delta(metas_cliente, f"{OUTPUT_PATH}/metas_por_cliente", "Metas por Cliente")
exportar_para_delta(modelo['dim_cliente'], f"{OUTPUT_PATH}/dim_cliente", "Dimensão Cliente")
exportar_para_delta(modelo['dim_produto'], f"{OUTPUT_PATH}/dim_produto", "Dimensão Produto")
exportar_para_delta(modelo['dim_tempo'], f"{OUTPUT_PATH}/dim_tempo", "Dimensão Tempo")
exportar_para_delta(modelo['fato_vendas'], f"{OUTPUT_PATH}/fato_vendas", "Fato Vendas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Criar Tabelas no Metastore (Opcional)

# COMMAND ----------

# Criar banco de dados se não existir
spark.sql("CREATE DATABASE IF NOT EXISTS analytics_vendas")

# Registrar tabelas
print("Registrando tabelas no Metastore...")

for nome_tabela, df in [
    ("visao_consolidada", visao_consolidada),
    ("metas_por_cliente", metas_cliente),
    ("dim_cliente", modelo['dim_cliente']),
    ("dim_produto", modelo['dim_produto']),
    ("dim_tempo", modelo['dim_tempo']),
    ("fato_vendas", modelo['fato_vendas'])
]:
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"analytics_vendas.{nome_tabela}")
    
    print(f"   Tabela analytics_vendas.{nome_tabela} criada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Resumo Final

# COMMAND ----------

print("=" * 80)
print("PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
print("=" * 80)
print(f"\nResumo da Execução:")
print(f"   • Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"   • Registros de vendas processados: {df_sales_clean.count():,}")
print(f"   • Clientes únicos: {modelo['dim_cliente'].count():,}")
print(f"   • Marcas/Produtos: {modelo['dim_produto'].count():,}")
print(f"   • Metas distribuídas: {metas_cliente.count():,} combinações")
print(f"\nDados salvos em: {OUTPUT_PATH}")
print(f"Tabelas criadas no banco: analytics_vendas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Visualizações Rápidas

# COMMAND ----------

# Top 10 clientes por volume
print("TOP 10 CLIENTES POR VOLUME")
display(
    visao_consolidada
    .orderBy(col("volume_medio_6m").desc())
    .limit(10)
)

# COMMAND ----------

# Distribuição de metas por marca
print("DISTRIBUIÇÃO DE METAS POR MARCA")
display(
    metas_cliente
    .groupBy("marca")
    .agg(
        sum("meta_cliente").alias("total_meta"),
        count("cliente_id").alias("num_clientes")
    )
    .orderBy(col("total_meta").desc())
)

# COMMAND ----------


