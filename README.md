# Sequoia - Cliente Spark para AWS Glue

Uma biblioteca Python simplificada para trabalhar com Apache Spark no AWS Glue, oferecendo uma interface limpa e eficiente para operações de dados.

## 🚀 Características Principais

- **Singleton Pattern**: Instância única do cliente
- **Configuração Otimizada**: Spark configurado automaticamente
- **Leitura Simplificada**: Um método para todos os formatos
- **Escrita Flexível**: Suporte a múltiplos formatos
- **Logging Integrado**: Sistema de logs robusto
- **Tratamento de Erros**: Gerenciamento de exceções

## 📁 Estrutura do Projeto

```
sequoia/
├── sequoia/
│   ├── __init__.py
│   ├── core.py          # Cliente principal
│   └── logger.py        # Sistema de logs
├── examples/
│   ├── config_examples.py
│   ├── file_operations_example.py
│   ├── singleton_example.py
│   ├── sql_join_example.py
│   └── union_example.py
├── docs/
│   └── index.html
├── main.py
├── requirements.txt
└── README.md
```

## 🛠️ Instalação

```bash
pip install -r requirements.txt
```

## 📖 Uso Básico

```python
from sequoia import GlueClient

# Inicializar cliente
sq = GlueClient()

# Ler tabela (funciona com qualquer formato)
df = sq.read_table("database", "table")

# Ler com filtros
df = sq.read_table(
    "database", 
    "table",
    columns=["id", "nome", "data"],
    where="data >= '2024-01-01'"
)

# Escrever arquivo
sq.write_file(df, "s3://bucket/dados.parquet")

# Executar SQL
df = sq.sql("SELECT * FROM database.table WHERE id > 100")
```

## 🔧 Métodos Principais

### Leitura de Dados
- **`read_table()`**: Lê tabelas do catálogo (qualquer formato)
- **`read_file()`**: Lê arquivos (Parquet, CSV, Excel)

### Escrita de Dados
- **`write_file()`**: Escreve DataFrames em arquivos
- **`write_table()`**: Escreve DataFrames como tabelas

### Configuração
- **`update_config()`**: Atualiza configurações Spark
- **`get_current_config()`**: Obtém configurações atuais

### Job Management
- **`job_init()`**: Inicializa job Glue
- **`job_commit()`**: Finaliza job Glue

### Utilitários
- **`sql()`**: Executa queries SQL
- **`createDataFrame()`**: Cria DataFrames
- **`get_table_info()`**: Obtém informações da tabela

## 🎯 Leitura Simplificada

O método `read_table()` funciona automaticamente com qualquer formato:

```python
# Funciona com Iceberg
df_iceberg = sq.read_table("database", "iceberg_table")

# Funciona com Parquet
df_parquet = sq.read_table("database", "parquet_table")

# Funciona com CSV
df_csv = sq.read_table("database", "csv_table")
```

## ⚙️ Configuração Otimizada do Spark

O Sequoia inicializa o Spark com configurações otimizadas:

```python
# Configurações automáticas incluídas:
# - KryoSerializer para performance
# - Configurações Parquet otimizadas
# - Configurações de memória e particionamento
# - Configurações de timezone
# - Configurações de checkpoint

# Adicionar configurações customizadas
sq.update_config({
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
})
```

## 📝 Exemplos

### Exemplo 1: Leitura Simples
```python
from sequoia import GlueClient

sq = GlueClient()
df = sq.read_table("meu_database", "minha_tabela")
print(f"Total de registros: {df.count()}")
```

### Exemplo 2: Leitura com Filtros
```python
df = sq.read_table(
    "meu_database", 
    "minha_tabela",
    columns=["id", "nome", "data"],
    where="data >= '2024-01-01'"
)
```

### Exemplo 3: Escrita de Arquivos
```python
# Parquet (padrão)
sq.write_file(df, "s3://bucket/dados.parquet")

# CSV
sq.write_file(df, "s3://bucket/dados.csv", format_type="csv")

# Com opções customizadas
sq.write_file(
    df, 
    "s3://bucket/dados.parquet",
    options={"compression": "gzip"}
)
```

### Exemplo 4: Queries SQL
```python
df = sq.sql("""
    SELECT 
        data,
        COUNT(*) as total
    FROM meu_database.minha_tabela
    WHERE data >= '2024-01-01'
    GROUP BY data
    ORDER BY data
""")
```

## 🔍 Tratamento de Erros

O Sequoia inclui tratamento robusto de erros:

```python
try:
    df = sq.read_table("database", "table")
except Exception as e:
    print(f"Erro ao ler tabela: {str(e)}")
```

## 📊 Logging

Sistema de logs integrado para debugging:

```python
# Logs automáticos para todas as operações
# Níveis: INFO, WARNING, ERROR
```

## 🚀 Performance

- **Configurações Otimizadas**: Spark configurado para performance
- **Singleton Pattern**: Evita múltiplas instâncias
- **Leitura Eficiente**: Otimizações automáticas
- **Memória Gerenciada**: Configurações de memória otimizadas

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature
3. Commit suas mudanças
4. Push para a branch
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo LICENSE para detalhes.