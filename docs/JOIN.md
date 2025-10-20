## ESTRUTURA ATUAL DO ETL

### 1. DADOS DE ENTRADA

**Tabela `sales.csv`:**
- `client_id` - ID do cliente
- `date` - Data da venda
- `brand` - Marca do produto
- `volume_hl` - Volume em hectolitros
- `b2b_status` - Status B2B (0 ou 1)
- `cep` - CEP do cliente

**Tabela `metas por marca.csv`:**
- `marca` - Nome da marca
- `meta` - Meta de vendas para a marca

---

### 2. MODELO DIMENSIONAL (STAR SCHEMA)

Estamos criando um **modelo dimensional clássico** com 3 dimensões e 1 fato:

#### **DIMENSÃO CLIENTE (`dim_cliente`)**
- `sk_cliente` (Surrogate Key) - Chave gerada
- `cliente_id` - ID original do cliente
- `cep` - CEP do cliente

#### **DIMENSÃO PRODUTO (`dim_produto`)**
- `sk_produto` (Surrogate Key) - Chave gerada
- `marca` - Nome da marca

#### **DIMENSÃO TEMPO (`dim_tempo`)**
- `sk_tempo` (Surrogate Key) - Chave gerada
- `date` - Data
- `ano` - Ano extraído
- `mes` - Mês extraído
- `trimestre` - Trimestre calculado

#### **FATO VENDAS (`fato_vendas`)**
- `sk_cliente` - FK para dim_cliente
- `sk_produto` - FK para dim_produto
- `sk_tempo` - FK para dim_tempo
- `data` - Data da transação
- `volume_hl` - Volume vendido
- `valor` - Valor da venda
- `b2b_status` - Status B2B

---

### 3. JOINS REALIZADOS

#### **JOIN 1: Criar Fato Vendas (Modelo Dimensional)**
```python
fato_vendas = df_sales \
    .join(dim_cliente, df_sales.client_id == dim_cliente.cliente_id, "left") \
    .join(dim_produto, df_sales.brand == dim_produto.marca, "left") \
    .join(dim_tempo, df_sales.date == dim_tempo.date, "left")
```

**Colunas usadas:**
- `df_sales.client_id` ⟷ `dim_cliente.cliente_id`
- `df_sales.brand` ⟷ `dim_produto.marca`
- `df_sales.date` ⟷ `dim_tempo.date`

#### **JOIN 2: Calcular Participação de Cliente por Marca**
```python
participacao = volume_cliente_marca.join(
    volume_total_marca, 
    "brand"
)
```

**Coluna usada:**
- `volume_cliente_marca.brand` ⟷ `volume_total_marca.brand`

#### **JOIN 3: Distribuir Metas**
```python
metas_distribuidas = participacao.join(
    df_metas.select(
        col("marca").alias("brand"),
        col("meta").alias("meta_marca")
    ),
    "brand"
)
```

**Coluna usada:**
- `participacao.brand` ⟷ `df_metas.marca` (renomeada como `brand`)

---

### 4. OUTPUTS FINAIS

1. **`visao_consolidada`** - Métricas agregadas por cliente (sem joins)
2. **`metas_por_cliente`** - Metas distribuídas proporcionalmente
3. **`dim_cliente`** - Dimensão de clientes
4. **`dim_produto`** - Dimensão de produtos/marcas
5. **`dim_tempo`** - Dimensão de tempo
6. **`fato_vendas`** - Fato central com as transações

---

### RESUMO DOS JOINS

| Tabela Esquerda | Tabela Direita | Coluna Join | Tipo |
|----------------|----------------|-------------|------|
| `df_sales` | `dim_cliente` | `client_id` = `cliente_id` | LEFT |
| `df_sales` | `dim_produto` | `brand` = `marca` | LEFT |
| `df_sales` | `dim_tempo` | `date` = `date` | LEFT |
| `volume_cliente_marca` | `volume_total_marca` | `brand` = `brand` | INNER |
| `participacao` | `df_metas` | `brand` = `marca` | INNER |
