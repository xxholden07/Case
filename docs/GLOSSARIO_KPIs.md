# Glossário de KPIs - Vendas Analytics

## Índice
- [Unidades de Medida](#unidades-de-medida)
- [KPIs de Volume](#kpis-de-volume)
- [KPIs de Comportamento](#kpis-de-comportamento)
- [KPIs de Segmentação](#kpis-de-segmentação)
- [KPIs de Metas](#kpis-de-metas)

---

## Unidades de Medida

### HL (Hectolitro)
- **Definição**: Unidade de volume equivalente a 100 litros
- **Conversão**: 1 HL = 100 litros = 0,1 m³
- **Uso**: Padrão da indústria de bebidas para medir volumes de produção e vendas
- **Exemplo**: 
  - 1 HL = 100 garrafas de 1L
  - 10 HL = 1.000 litros
  - 100 HL = 10.000 litros

### CEP
- **Definição**: Código de Endereçamento Postal
- **Formato**: 8 dígitos (XXXXX-XXX)
- **Uso**: Identificação geográfica do cliente
- **Fonte**: API ViaCEP (https://viacep.com.br/)

---

## KPIs de Volume

### Volume Médio 3M (volume_medio_3m)
- **Definição**: Média de volume de compras nos últimos 3 meses
- **Unidade**: HL (Hectolitros)
- **Cálculo**: Soma do volume dos últimos 3 meses ÷ 3
- **Objetivo**: Identificar tendência recente de consumo
- **Interpretação**:
  - **Alto (>50 HL)**: Cliente de alto volume, atenção prioritária
  - **Médio (10-50 HL)**: Cliente regular, potencial de crescimento
  - **Baixo (<10 HL)**: Cliente pontual ou em declínio

### Volume Médio 6M (volume_medio_6m)
- **Definição**: Média de volume de compras nos últimos 6 meses
- **Unidade**: HL (Hectolitros)
- **Cálculo**: Soma do volume dos últimos 6 meses ÷ 6
- **Objetivo**: Avaliar tendência de médio prazo
- **Uso**: Comparar com volume 3M para identificar sazonalidade

**Análise Comparativa 3M vs 6M:**
- **3M > 6M**: Cliente em crescimento recente
- **3M < 6M**: Cliente em declínio, risco de churn
- **3M ≈ 6M**: Cliente estável, consumo constante

---

## KPIs de Comportamento

### Data Última Compra (data_ultima_compra)
- **Definição**: Data da transação mais recente do cliente
- **Formato**: YYYY-MM-DD
- **Objetivo**: Identificar clientes inativos ou em risco
- **Ações**:
  - **< 30 dias**: Cliente ativo
  - **30-90 dias**: Cliente em risco, ação de retenção
  - **> 90 dias**: Cliente inativo, campanha de reativação

### Meses Desde Última Compra (meses_desde_ultima_compra)
- **Definição**: Número de meses desde a última transação
- **Cálculo**: MONTHS_BETWEEN(data_atual, data_ultima_compra)
- **Uso**: Segmentação de clientes por recência
- **Classificação RFM**:
  - **0-1 mês**: Recência Alta (score 5)
  - **2-3 meses**: Recência Média-Alta (score 4)
  - **4-6 meses**: Recência Média (score 3)
  - **7-12 meses**: Recência Baixa (score 2)
  - **>12 meses**: Recência Muito Baixa (score 1)

---

## KPIs de Segmentação

### Share B2B 3M (share_b2b_3m)
- **Definição**: Percentual de volume B2B nos últimos 3 meses
- **Unidade**: % (Percentual)
- **Cálculo**: (Volume B2B últimos 3 meses ÷ Volume Total últimos 3 meses) × 100
- **Interpretação**:
  - **100%**: Cliente exclusivamente B2B (atacado/distribuidor)
  - **50-99%**: Cliente majoritariamente B2B
  - **1-49%**: Cliente majoritariamente B2C (varejo final)
  - **0%**: Cliente exclusivamente B2C

### Share B2B 6M (share_b2b_6m)
- **Definição**: Percentual de volume B2B nos últimos 6 meses
- **Unidade**: % (Percentual)
- **Cálculo**: (Volume B2B últimos 6 meses ÷ Volume Total últimos 6 meses) × 100
- **Uso**: Identificar mudanças no padrão de compra

**Análise de Mudança de Canal:**
- **Share B2B 3M > 6M**: Cliente migrando para atacado
- **Share B2B 3M < 6M**: Cliente migrando para varejo
- **Share estável**: Padrão de compra consistente

### Tipo de Cliente (B2B vs B2C)
- **B2B (Business to Business)**:
  - Distribuidores, atacadistas, revendedores
  - Volumes maiores, margens menores
  - Compras regulares e planejadas
  
- **B2C (Business to Consumer)**:
  - Varejo final, consumidor direto
  - Volumes menores, margens maiores
  - Compras pontuais e esporádicas

---

## KPIs de Metas

### Meta Marca (meta_marca)
- **Definição**: Volume total planejado para a marca no ano
- **Unidade**: HL (Hectolitros)
- **Origem**: Arquivo "metas por marca.csv"
- **Uso**: Acompanhamento de desempenho comercial

### Meta Cliente (meta_cliente)
- **Definição**: Parcela da meta da marca distribuída para cada cliente
- **Unidade**: HL (Hectolitros)
- **Cálculo**: Meta Marca × (% Participação Histórica do Cliente)
- **Método de Distribuição**:
  1. Calcular volume histórico de cada cliente por marca
  2. Calcular % de participação do cliente na marca
  3. Distribuir meta proporcionalmente

**Exemplo de Distribuição:**
```
Meta Marca X: 1.000 HL
- Cliente A vendeu 600 HL historicamente (60%) → Meta: 600 HL
- Cliente B vendeu 300 HL historicamente (30%) → Meta: 300 HL
- Cliente C vendeu 100 HL historicamente (10%) → Meta: 100 HL
Total: 1.000 HL ✓
```

### Percentual de Participação (percentual_participacao)
- **Definição**: Representatividade do cliente no volume total da marca
- **Unidade**: % (Percentual)
- **Cálculo**: (Volume Cliente Marca ÷ Volume Total Marca) × 100
- **Uso**: Priorização de clientes estratégicos

**Classificação ABC:**
- **Classe A (>50%)**: Clientes estratégicos, alta participação
- **Classe B (20-50%)**: Clientes importantes
- **Classe C (<20%)**: Clientes complementares

---

## Análises Estratégicas

### RFM (Recency, Frequency, Monetary)
Combinação de métricas para segmentação de clientes:

- **Recency**: Meses desde última compra
- **Frequency**: Número de transações no período
- **Monetary**: Volume médio de compras (3M ou 6M)

**Segmentos:**
- **Champions**: R=Alto, F=Alto, M=Alto (clientes top)
- **Loyal**: R=Alto, F=Alto, M=Médio (clientes fiéis)
- **At Risk**: R=Baixo, F=Alto, M=Alto (clientes em risco)
- **Lost**: R=Muito Baixo, F=Qualquer, M=Qualquer (clientes perdidos)

### Análise de Tendência
- **Volume 3M vs 6M**: Identificar crescimento ou declínio
- **Share B2B 3M vs 6M**: Detectar mudança de canal
- **Meta vs Realizado**: Avaliar performance comercial

### Análise Geográfica
- **Cidade**: Concentração de vendas por localidade
- **Estado**: Distribuição regional
- **Uso**: Estratégias de logística e expansão

---

## Alertas e Thresholds

### Volume
- ⚠️ **Volume 3M < 50% Volume 6M**: Cliente em declínio acentuado
- ✅ **Volume 3M > 120% Volume 6M**: Cliente em crescimento acelerado

### Recência
- 🔴 **>90 dias sem compra**: Cliente inativo, ação urgente
- 🟡 **60-90 dias sem compra**: Cliente em risco
- 🟢 **<30 dias**: Cliente ativo

### Metas
- 🔴 **Realizado < 50% Meta**: Performance crítica
- 🟡 **Realizado 50-80% Meta**: Performance abaixo do esperado
- 🟢 **Realizado > 100% Meta**: Performance excelente

---

## Glossário Técnico

- **Star Schema**: Modelo dimensional com fato central e dimensões
- **Janela Rolling**: Cálculo de média móvel em período deslizante
- **ETL**: Extract, Transform, Load - processo de integração de dados
- **PySpark**: Framework de processamento distribuído de dados em Python
- **SQLite**: Banco de dados relacional embutido
- **API ViaCEP**: Serviço REST para consulta de endereços por CEP

---

## Fontes de Dados

1. **sales.csv**: Transações históricas de vendas
2. **metas por marca.csv**: Metas anuais por marca
3. **API ViaCEP**: Enriquecimento geográfico (cidade/estado)

---

## Periodicidade de Atualização

- **ETL**: Sob demanda (executar `.\run_etl.ps1`)
- **Dashboard**: Tempo real (conectado ao SQLite)
- **Metas**: Anual (atualizar arquivo CSV)
- **Dados de CEP**: Uma vez (cache após primeira consulta)

---

## Contato

Para dúvidas sobre KPIs ou metodologia de cálculo, consulte:
- Documentação técnica: `docs/FLUXOGRAMA_ETL.md`
- Estrutura do projeto: `docs/ESTRUTURA_PROJETO.md`
- Logs do ETL: `logs/etl_YYYYMMDD_HHMMSS.log`
