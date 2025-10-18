# Fluxograma do ETL - Vendas Analytics

```mermaid
flowchart TD
    Start([Início ETL]) --> CheckFiles{sales.csv existe?}
    
    CheckFiles -->|Não| ConvertExcel[Converter XLSX → CSV<br/>usando Pandas]
    CheckFiles -->|Sim| LoadData
    ConvertExcel --> LoadData
    
    LoadData[Carregar Dados<br/>- sales.csv PySpark CSV reader<br/>- metas por marca.csv separador ponto-vírgula]
    
    LoadData --> Explore[Exploração de Dados<br/>- Exibir Schema<br/>- Estatísticas descritivas<br/>- Contagem de nulos por coluna]
    
    Explore --> Clean[Limpeza de Dados<br/>✓ Converter data_doc para DateType<br/>✓ Tratar valores nulos<br/>✓ Validar tipos de dados]
    
    Clean --> ValidateClean{Dados limpos<br/>sem erros?}
    ValidateClean -->|Não| ErrorLog1[LOG: Erro na limpeza]
    ValidateClean -->|Sim| CreateDim
    ErrorLog1 --> End
    
    CreateDim[Criar Modelo Dimensional<br/>- dim_cliente: extrair clientes únicos<br/>- dim_produto: extrair produtos únicos<br/>- fato_vendas: transações com FKs]
    
    CreateDim --> ValidateDim{Modelo dimensional<br/>consistente?}
    ValidateDim -->|Não| ErrorLog2[LOG: Erro no modelo]
    ValidateDim -->|Sim| CreateView
    ErrorLog2 --> End
    
    CreateView[Criar Visão Consolidada<br/>- Identificar última compra<br/>- Calcular meses desde última compra<br/>- Agregar métricas por cliente<br/>- Métricas rolling janela 3M<br/>- Métricas rolling janela 6M<br/>- Calcular share B2B por período]
    
    CreateView --> ValidateView{Clientes com<br/>métricas válidas?}
    ValidateView -->|Não| ErrorLog3[LOG: Métricas zeradas<br/>Verificar conversão de datas]
    ValidateView -->|Sim| DistributeMetas
    ErrorLog3 --> End
    
    DistributeMetas[Distribuir Metas por Cliente<br/>- Filtrar metas do ano parametrizado<br/>- Calcular volume histórico cliente-marca<br/>- Calcular % participação de cada cliente<br/>- Distribuir meta proporcional ao histórico]
    
    DistributeMetas --> ValidateMetas{Validação:<br/>Soma = Meta marca?}
    ValidateMetas -->|Não| ErrorLog4[LOG: Diferença na soma]
    ValidateMetas -->|Sim| Enrich
    ErrorLog4 --> End
    
    Enrich[Enriquecimento de CEP<br/>Buscar endereços via API ViaCEP<br/>Processo opcional]
    
    Enrich --> Export[Exportação Multi-formato]
    
    Export --> ExportCSV[Exportar CSVs<br/>output/csv/]
    ExportCSV --> ExportParquet[Exportar Parquet<br/>output/parquet/]
    ExportParquet --> ExportSQLite[Exportar SQLite<br/>output/vendas_analytics.db]
    
    ExportSQLite --> ValidateExport{Todos os arquivos<br/>criados?}
    ValidateExport -->|Não| ErrorLog5[LOG: Falha na exportação]
    ValidateExport -->|Sim| CreateIndices
    ErrorLog5 --> End
    
    CreateIndices[Criar Índices SQLite<br/>- Índices em chaves primárias<br/>- Índices em chaves estrangeiras<br/>- Índices para queries de filtro]
    
    CreateIndices --> LogSummary[LOG: Resumo do ETL<br/>- Tempo total de execução<br/>- Total de registros processados<br/>- Arquivos gerados<br/>- Warnings e erros]
    
    LogSummary --> End([ETL Concluído ✓])
    
    style Start fill:#4CAF50,stroke:#2E7D32,color:#fff
    style End fill:#4CAF50,stroke:#2E7D32,color:#fff
    style ErrorLog1 fill:#F44336,stroke:#C62828,color:#fff
    style ErrorLog2 fill:#F44336,stroke:#C62828,color:#fff
    style ErrorLog3 fill:#F44336,stroke:#C62828,color:#fff
    style ErrorLog4 fill:#F44336,stroke:#C62828,color:#fff
    style ErrorLog5 fill:#F44336,stroke:#C62828,color:#fff
    style ValidateClean fill:#FFC107,stroke:#F57C00
    style ValidateDim fill:#FFC107,stroke:#F57C00
    style ValidateView fill:#FFC107,stroke:#F57C00
    style ValidateMetas fill:#FFC107,stroke:#F57C00
    style ValidateExport fill:#FFC107,stroke:#F57C00
```

## Legenda

### Entrada
- **sales.csv**: Arquivo de transações de vendas
- **metas por marca.csv**: Arquivo de metas por marca e ano

### Processamento
1. **Limpeza**: Conversão de tipos, tratamento de nulos, validação de dados
2. **Modelo Dimensional**: Star Schema com dimensões e fato
3. **Visão Consolidada**: Agregação de métricas por cliente com janelas temporais
4. **Distribuição de Metas**: Alocação proporcional baseada no histórico de vendas

### Saída
- **CSV**: Arquivos texto delimitados em `output/csv/`
- **Parquet**: Arquivos colunares otimizados em `output/parquet/`
- **SQLite**: Banco de dados relacional em `output/vendas_analytics.db`

### Pontos de Validação
- Dados limpos sem nulos em campos críticos
- Modelo dimensional com integridade referencial
- Métricas rolling calculadas corretamente
- Soma das metas distribuídas igual à meta da marca
- Todos os formatos de saída gerados

### Níveis de Log
- **INFO**: Progresso normal do ETL
- **WARNING**: Situações de atenção (queries vazias, valores nulos)
- **ERROR**: Falhas críticas que interrompem o processo
