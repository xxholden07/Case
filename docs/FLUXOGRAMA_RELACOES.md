# FLUXOGRAMA DE RELAÇÕES DAS TABELAS

## Diagrama de Relacionamentos - Modelo Star Schema

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DADOS DE ENTRADA (RAW)                              │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌──────────────────┐              ┌──────────────────────┐
     │   sales.csv      │              │ metas por marca.csv  │
     ├──────────────────┤              ├──────────────────────┤
     │ • client_id      │              │ • marca              │
     │ • date           │              │ • meta               │
     │ • brand          │              └──────────────────────┘
     │ • volume_hl      │                       │
     │ • b2b_status     │                       │
     │ • cep            │                       │
     └──────────────────┘                       │
            │                                   │
            │ [TRANSFORMAÇÃO]                   │
            ▼                                   │
                                               │
┌─────────────────────────────────────────────────────────────────────────────┐
│                      MODELO DIMENSIONAL (STAR SCHEMA)                        │
└─────────────────────────────────────────────────────────────────────────────┘

        [DIMENSÕES]                           [FATO]

    ┌────────────────┐
    │ dim_cliente    │
    ├────────────────┤
    │ PK sk_cliente  │◄─────────┐
    │    cliente_id  │           │
    │    cep         │           │
    └────────────────┘           │
                                 │
    ┌────────────────┐           │      ┌─────────────────────┐
    │ dim_produto    │           │      │   fato_vendas       │
    ├────────────────┤           │      ├─────────────────────┤
    │ PK sk_produto  │◄──────────┼──────┤ FK sk_cliente       │
    │    marca       │           │      │ FK sk_produto       │
    └────────────────┘           │      │ FK sk_tempo         │
                                 └──────┤    data             │
    ┌────────────────┐           ┌──────┤    volume_hl        │
    │ dim_tempo      │           │      │    valor            │
    ├────────────────┤           │      │    b2b_status       │
    │ PK sk_tempo    │◄──────────┘      └─────────────────────┘
    │    date        │
    │    ano         │
    │    mes         │
    │    trimestre   │
    └────────────────┘


┌─────────────────────────────────────────────────────────────────────────────┐
│                    CÁLCULO DE DISTRIBUIÇÃO DE METAS                          │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────────┐
    │  fato_vendas         │
    │  [Agregação]         │
    └──────────────────────┘
             │
             │ GROUP BY client_id, brand
             ▼
    ┌──────────────────────┐
    │ volume_cliente_marca │
    ├──────────────────────┤
    │ • client_id          │
    │ • brand              │
    │ • volume_cliente     │
    └──────────────────────┘
             │
             │ JOIN: brand = brand
             ├──────────────────────┐
             ▼                      ▼
    ┌──────────────────────┐   ┌──────────────────────┐
    │ volume_total_marca   │   │   participacao       │
    ├──────────────────────┤   ├──────────────────────┤
    │ • brand              │   │ • client_id          │
    │ • volume_total       │   │ • brand              │
    └──────────────────────┘   │ • volume_cliente     │
             │                 │ • volume_total       │
             │                 │ • perc_participacao  │
             │                 └──────────────────────┘
             │                          │
             │                          │ JOIN: brand = marca
             │                          ├─────────────────┐
             │                          ▼                 ▼
             │                 ┌──────────────────┐   ┌──────────────────────┐
             └─────────────────┤ metas por marca  │   │ metas_distribuidas   │
                               ├──────────────────┤   ├──────────────────────┤
                               │ • marca          │   │ • client_id          │
                               │ • meta           │   │ • brand              │
                               └──────────────────┘   │ • volume_cliente     │
                                                      │ • volume_total       │
                                                      │ • perc_participacao  │
                                                      │ • meta_marca         │
                                                      │ • meta_cliente       │
                                                      └──────────────────────┘
                                                                │
                                                                ▼
                                                      ┌──────────────────────┐
                                                      │ metas_por_cliente    │
                                                      │ [OUTPUT FINAL]       │
                                                      └──────────────────────┘
```

---

## Detalhamento dos JOINs

### 1️⃣ **Construção do Fato de Vendas (Star Schema)**

```
sales.csv ──┬─→ [LEFT JOIN] ──→ dim_cliente  (ON: client_id = cliente_id)
            │
            ├─→ [LEFT JOIN] ──→ dim_produto  (ON: brand = marca)
            │
            └─→ [LEFT JOIN] ──→ dim_tempo     (ON: date = date)
                    │
                    ▼
              fato_vendas
```

**Tipo:** LEFT JOIN  
**Cardinalidade:** N:1 (Muitas vendas para um cliente/produto/tempo)  
**Objetivo:** Criar a tabela fato enriquecida com as surrogate keys das dimensões

---

### 2️⃣ **Agregação de Volume por Cliente e Marca**

```
fato_vendas
     │
     │ [GROUP BY client_id, brand]
     │ [SUM(volume_hl)]
     ▼
volume_cliente_marca
```

**Tipo:** Agregação (não é JOIN)  
**Objetivo:** Calcular volume total de cada cliente por marca

---

### 3️⃣ **Cálculo de Participação**

```
volume_cliente_marca ──→ [INNER JOIN] ──→ volume_total_marca
                              │                (ON: brand = brand)
                              ▼
                        participacao
                    [volume_cliente / volume_total]
```

**Tipo:** INNER JOIN  
**Cardinalidade:** N:1 (Muitos clientes para uma marca)  
**Objetivo:** Calcular percentual de participação de cada cliente na marca

---

### 4️⃣ **Distribuição de Metas**

```
participacao ──→ [INNER JOIN] ──→ metas por marca
                      │              (ON: brand = marca)
                      ▼
              metas_distribuidas
          [meta_cliente = meta_marca × perc_participacao]
                      │
                      ▼
              metas_por_cliente
```

**Tipo:** INNER JOIN  
**Cardinalidade:** N:1 (Muitos clientes para uma meta de marca)  
**Objetivo:** Distribuir proporcionalmente a meta da marca entre os clientes

---

## Legenda

- **PK** = Primary Key (Chave Primária)
- **FK** = Foreign Key (Chave Estrangeira)
- **SK** = Surrogate Key (Chave Substituta)
- **→** = Fluxo de dados
- **◄─** = Relacionamento/JOIN

---

## Outputs Finais

```
OUTPUT
  ├─ dim_cliente.csv/parquet        [Dimensão]
  ├─ dim_produto.csv/parquet        [Dimensão]
  ├─ dim_tempo.csv/parquet          [Dimensão]
  ├─ fato_vendas.csv/parquet        [Fato]
  ├─ metas_por_cliente.csv/parquet  [Análise]
  └─ visao_consolidada.csv/parquet  [Análise Agregada]
```

---

## Cardinalidades

| Relacionamento | Tipo | Descrição |
|---------------|------|-----------|
| `sales → dim_cliente` | N:1 | Muitas vendas por cliente |
| `sales → dim_produto` | N:1 | Muitas vendas por produto |
| `sales → dim_tempo` | N:1 | Muitas vendas por data |
| `volume_cliente_marca → volume_total_marca` | N:1 | Muitos clientes por marca |
| `participacao → metas por marca` | N:1 | Muitos clientes por meta de marca |

---

