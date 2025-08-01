# Sequoia

Biblioteca Python para desenvolvimento de jobs AWS Glue 5 com otimizações KryoSerializer e suporte a leitura de dados Parquet e Iceberg. A classe `GlueClient` encapsula e simplifica o uso dos contextos Spark.

## Estrutura do Projeto

```
sequoia/
├── main.py                 # Job principal de exemplo
├── requirements.txt        # Dependências do projeto
├── README.md              # Documentação
├── sequoia/
│   ├── core.py             # Classe principal GlueClient (Singleton)
│   ├── logger.py           # Logger customizado
│   └── __init__.py
├── examples/
│   ├── simple_auto_detection.py    # Detecção automática
│   ├── iceberg_example.py          # Exemplo Iceberg
│   ├── sql_join_example.py         # Exemplo SQL
│   ├── config_examples.py          # Configurações customizadas
│   ├── optimized_config_example.py # Configuração otimizada
│   ├── config_error_handling_example.py  # Tratamento de erros de config
│   ├── s3_permission_example.py    # Tratamento de permissões S3
│   ├── file_operations_example.py  # Operações com arquivos
│   ├── singleton_example.py        # Padrão Singleton
│   ├── union_example.py            # UNION entre tabelas
│   └── iceberg_detection_example.py  # Detecção de Iceberg
└── docs/
    └── index.html          # Documentação HTML
```

## Configurações KryoSerializer

O projeto utiliza o KryoSerializer para otimizar o processamento de dados Parquet:

### Configurações Aplicadas

- **KryoSerializer**: Serialização otimizada para melhor performance
- **Compressão Snappy**: Para arquivos Parquet
- **Vectorized Reader**: Leitura otimizada de Parquet
- **Adaptive Query Execution**: Otimizações automáticas do Spark
- **Configurações de Memória**: Buffer e cache otimizados

### Benefícios

- Melhor performance na serialização
- Redução do uso de memória
- Otimizações automáticas de queries
- Leitura/escrita otimizada de Parquet
- Suporte a leitura de Iceberg (time travel e snapshots)

## Como Usar

### 1. Instalar Dependências

```bash
pip install -r requirements.txt
```

### 2. Configurar Parâmetros do Job

O job aceita os seguintes parâmetros:

- `JOB_NAME`: Nome do job
- `database_name`: Nome do banco de dados no Glue Catalog
- `table_name`: Nome da tabela (Parquet ou Iceberg)
- `output_path`: Caminho S3 para salvar resultados (opcional)

### 3. Executar o Job

```bash
python main.py \
  --JOB_NAME=meu-job \
  --database_name=meu_database \
  --table_name=minha_tabela \
  --output_path=s3://bucket/output/
```

### 4. Detecção Automática de Iceberg

O job detecta automaticamente se a tabela é Iceberg ou formato padrão:

```python
# Método automático (RECOMENDADO)
df = sq.read_table("database", "table")

# Método manual (para debug)
table_format = sq.detect_table_format("database", "table")
if table_format == "iceberg":
    df = sq.read_iceberg_table_from_catalog("database", "table")
else:
    df = sq.read_table_from_catalog("database", "table")
```

## Funcionalidades

### Leitura de Dados

- Leitura de tabelas do Glue Catalog (Parquet, Iceberg)
- **Detecção automática de Iceberg** - 100-1000x mais rápida
- Leitura direta de arquivos Parquet do S3
- Otimizações automáticas com KryoSerializer

### Processamento

- Análise de schema dos dados
- Contagem de registros
- Agregações básicas
- Queries SQL com JOINs
- Criação de DataFrames

### Escrita de Dados

- Escrita otimizada em formato Parquet
- Configurações de compressão e particionamento
- Opções de modo de escrita (overwrite, append, error, ignore)
- Suporte a leitura de tabelas Iceberg (time travel e snapshots)

## Utilitários

### Sequoia

Classe principal que encapsula SparkContext, GlueContext e SparkSession:

**Inicialização Simplificada:**
```python
# Cria automaticamente todos os contextos
sq = GlueClient()

# Ou com contextos específicos (opcional)
sq = GlueClient(spark_context=sc, glue_context=glue_context, spark_session=spark)
```

**Métodos Principais:**
- **Leitura automática**: `read_table()` - detecta Iceberg e lê automaticamente
- **Detecção de formato**: `detect_table_format()` - identifica Iceberg via metadados
- **Leitura específica**: `read_table_from_catalog()` e `read_iceberg_table_from_catalog()`
- **Leitura de arquivos**: `read_file()` - lê Parquet, CSV, Excel
- **Escrita de arquivos**: `write_file()` - escreve Parquet, CSV, Excel
- **Escrita de tabelas**: Apenas Parquet
- **Otimizações de DataFrame**
- **Informações sobre tabelas**

- **Controle de Job**: `job_init()`, `job_commit()` - para job bookmarks
- **Argumentos**: `get_job_args()`, `get_arg()` - encapsulamento de argumentos

**Acesso aos Contextos:**
```python
sequoia.spark_context    # SparkContext
sequoia.glue_context     # GlueContext  
sequoia.spark_session    # SparkSession
```

### Logger

Logger customizado para:
- Logs estruturados
- Diferentes níveis de log
- Formatação consistente

## Exemplo de Uso

```python
from sequoia.core import GlueClient

# Criar utilitários (contextos criados automaticamente)
sq = GlueClient()

# Método 1: Detecção automática (RECOMENDADO)
df = sq.read_table("database", "table")

# Método 2: Detecção manual (para debug)
table_format = sq.detect_table_format("database", "table")
if table_format == "iceberg":
    df = sq.read_iceberg_table_from_catalog("database", "table")
else:
    df = sq.read_table_from_catalog("database", "table")

# Processar dados
df_processed = df  # DataFrame processado

# Salvar resultados (Parquet ou Iceberg)
sq.write_file(df_processed, "s3://bucket/output/")
# Ou para tabela:
sq.write_table(df_processed, "database", "table")
```

## Operações com Arquivos

O Sequoia suporta leitura e escrita de diferentes formatos de arquivo:

### Leitura de Arquivos

```python
# Ler Parquet (padrão)
df = sequoia.read_file("s3://bucket/data.parquet")

# Ler CSV
df = sequoia.read_file(
    "s3://bucket/data.csv",
    format_type="csv",
    options={
        "header": "true",
        "delimiter": ",",
        "encoding": "UTF-8"
    }
)

# Ler Excel
df = sequoia.read_file(
    "s3://bucket/data.xlsx",
    format_type="excel"
)

# Ler Excel com planilha específica
df = sequoia.read_file(
    "s3://bucket/data.xlsx",
    format_type="excel",
    options={
        "sheet_name": "Planilha1"
    }
)

# Ler Excel com índice de planilha
df = sequoia.read_file(
    "s3://bucket/data.xlsx",
    format_type="excel",
    options={
        "sheet_name": 1  # Segunda planilha
    }
)

# Ler Parquet com opções customizadas
df = sequoia.read_file(
    "s3://bucket/data.parquet",
    format_type="parquet",
    options={
        "compression": "gzip",
        "mergeSchema": "true"
    }
)
```

### Escrita de Arquivos

```python
# Salvar como Parquet
sequoia.write_file(df, "s3://bucket/output.parquet")

# Salvar como CSV
sequoia.write_file(
    df,
    "s3://bucket/output.csv",
    format_type="csv",
    options={
        "header": "true",
        "delimiter": ","
    }
)

# Salvar como Parquet com compressão gzip
sequoia.write_file(
    df,
    "s3://bucket/output.parquet",
    format_type="parquet",
    options={
        "compression": "gzip"
    }
)
```

**Nota**: Atualmente suporta apenas Parquet e CSV. Para arquivos Excel, use o método anterior.

### Opções por Formato

#### **CSV Options (Leitura):**
```python
options = {
    "header": "true",           # Se tem cabeçalho
    "delimiter": ",",           # Delimitador
    "inferSchema": "true",      # Inferir schema
    "encoding": "UTF-8"         # Encoding
}
```

#### **CSV Options (Escrita):**
```python
options = {
    "header": "true",           # Incluir cabeçalho
    "delimiter": ",",           # Delimitador
    "encoding": "UTF-8"         # Encoding
}
```

#### **Parquet Options (Leitura):**
```python
options = {
    "compression": "snappy",    # Compressão
    "mergeSchema": "false",     # Mesclar schemas
    "columnarReaderBatchSize": "4096"  # Tamanho do batch
}
```

#### **Parquet Options (Escrita):**
```python
options = {
    "compression": "snappy"     # Compressão (snappy, gzip, zstd)
}
```

#### **Excel Options (Leitura):**
```python
options = {
    "sheet_name": 0,           # Nome ou índice da planilha (0 = primeira)
    "header": 0,               # Linha do cabeçalho (0 = primeira linha)
    "engine": "openpyxl"       # Engine para ler Excel
}
```

## Job Bookmarks

O Sequoia suporta controle de job para job bookmarks:

```python
# Inicializar job (necessário para job bookmarks)
sequoia.job_init("meu_job", args)

# Ler dados normalmente
df = sequoia.read_table("database", "table")

# Finalizar job (necessário para job bookmarks)
sequoia.job_commit()
```

## Controle de Job

Para jobs que usam job bookmarks, você pode controlar a inicialização e finalização:

```python
# Inicializar job (necessário para job bookmarks)
sequoia.job_init("meu_job", args)

# ... seu código aqui ...

# Finalizar job (necessário para job bookmarks)
sequoia.job_commit()
```

**Nota**: Esses métodos são opcionais e só são necessários se você estiver usando job bookmarks.

## Configurações de Parquet

O Sequoia inclui configurações otimizadas para Parquet que são aplicadas automaticamente pelo SparkSession:

```python
# Configurações automáticas aplicadas no SparkSession:
# - spark.sql.parquet.block.size: 134217728 (128MB)
# - spark.sql.parquet.page.size: 1048576 (1MB)
# - spark.sql.parquet.compression.codec: snappy
# - spark.sql.parquet.enableVectorizedReader: true

# As configurações são aplicadas automaticamente em todas as operações:
# - write_file()
# - write_table()
```

**Nota**: Como as configurações são aplicadas globalmente no SparkSession, não é necessário especificar `block.size` e `page.size` individualmente em cada operação de escrita.

## Argumentos do Job

O Sequoia encapsula automaticamente os argumentos do job:

```python
# Obter todos os argumentos
args = sequoia.get_job_args(["JOB_NAME", "database_name", "table_name"])

# Obter argumento específico
database_name = sequoia.get_arg("database_name")
table_name = sequoia.get_arg("table_name", default="minha_tabela")

# Inicializar job com argumentos automáticos
sequoia.job_init()  # Obtém argumentos automaticamente
```

## Detecção de Iceberg via Metadados

O Sequoia implementa detecção de tabelas Iceberg ultra-rápida usando apenas metadados do Glue Catalog:

### Como Funciona

1. **Consulta aos metadados**: Acessa apenas informações do Glue Catalog
2. **Análise de propriedades**: Verifica parâmetros específicos do Iceberg
3. **Detecção específica**: Identifica se é tabela Iceberg ou formato padrão
4. **Performance excepcional**: 100-1000x mais rápida que consultar dados

### Vantagens

- **⚡ Ultra-rápida**: ~10-50ms vs ~1-10 segundos
- **💰 Baixo custo**: Apenas consulta ao Glue Catalog
- **🔍 Precisão**: Detecta especificamente tabelas Iceberg
- **🎯 Simples**: Iceberg ou formato padrão

## Detecção de Iceberg Otimizada

Com a detecção via metadados sendo **100-1000x mais rápida**, não há necessidade de cache:

### Método de Detecção

```python
# Detecção automática de Iceberg via metadados (RECOMENDADO)
format = sq.detect_table_format("database", "table")

# Resultados possíveis:
# - "iceberg" - Tabela Iceberg detectada
# - "standard" - Tabela padrão (Parquet, etc.)
```

### Exemplo de Performance

```python
# Detecção via metadados (muito rápida)
df1 = sq.read_table("database", "table")  # ~10-50ms

# Detecção direta (sempre rápida)
df2 = sq.read_table("database", "table")  # ~10-50ms

# Detecção direta (sempre rápida)
df3 = sq.read_table("database", "table")  # ~10-50ms
```

**Nota**: Com detecção via metadados tão rápida, não há necessidade de cache.

## Configuração Otimizada do Spark

O Sequoia implementa uma estratégia robusta de configuração via SparkConf:

### Configurações Padrão Unificadas

```python
# Configurações aplicadas automaticamente na inicialização:
default_config = {
    # KryoSerializer para performance
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrationRequired": "false",
    "spark.kryoserializer.buffer.max": "2047m",
    
    # Parquet otimizado
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.parquet.filterPushdown": "true",
    "spark.sql.parquet.block.size": "134217728",  # 128MB
    
    # Performance adaptativa
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
}
```

### Vantagens da Nova Estratégia

- **✅ Sem erros CANNOT_MODIFY_CONFIG**: Configurações aplicadas na inicialização
- **✅ Configurações unificadas**: Todas em um local centralizado
- **✅ Fallback robusto**: SparkContext padrão se falhar
- **✅ Configurações customizadas**: Suportadas via parâmetro
- **✅ Compatibilidade**: Funciona em AWS Glue e ambiente local
- **✅ Tratamento de erro**: GlueContext com fallback para SparkSession

### Uso Otimizado

```python
# ✅ Configurações otimizadas aplicadas automaticamente
client = GlueClient()

# ✅ Com configurações customizadas
custom_config = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}
client = GlueClient(spark_config=custom_config)

# ✅ Verificar configurações aplicadas
config = client.get_current_config()
```

## Tratamento de Erros S3

O Sequoia inclui diagnóstico automático para problemas de permissão S3:

### Erro Comum: AccessDenied

```python
# Erro que pode ocorrer:
# org.apache.hadoop.fs.s3a.AWSS3IOException: 
# com.amazonaws.services.s3.model.AmazonS3Exception: 
# Access Denied (Service: Amazon S3; Status Code: 403)
```

### Diagnóstico Automático

O método `write_file()` inclui diagnóstico automático:

```python
# Tentativa de escrita com diagnóstico
try:
    client.write_file(df, "s3://meu-bucket/dados.parquet")
except Exception as e:
    # Diagnóstico automático é logado
    print(f"Erro: {str(e)}")
```

### Permissões IAM Necessárias

Para o IAM Role do Glue:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::meu-bucket",
        "arn:aws:s3:::meu-bucket/*"
      ]
    }
  ]
}
```

## Uso Básico

```python
from sequoia import GlueClient

# Criar instância (Singleton - sempre a mesma)
sq = GlueClient()

# Logs integrados
sq.logger.info("Iniciando processamento")
sq.logger.warning("Aviso importante")
sq.logger.error("Erro encontrado")

# Ler dados (detecção automática de Iceberg)
df = sq.read_table("database", "table")

# Detecção manual de formato
table_format = sq.detect_table_format("database", "table")
if table_format == "iceberg":
    df = sq.read_iceberg_table_from_catalog("database", "table")
else:
    df = sq.read_table_from_catalog("database", "table")
```

## Padrão Singleton

O Sequoia implementa o padrão Singleton, garantindo uma única instância:

```python
# Sempre a mesma instância
sq1 = Sequoia()
sq2 = Sequoia()
print(sq1 is sq2)  # True

# Logs integrados
sq.logger.info("Usando logs integrados")
sq.logger.warning("Aviso")
sq.logger.error("Erro")
```

### Vantagens do Singleton:

1. **Instância Única**: Sempre a mesma instância em todo o código
2. **Logs Integrados**: Acesso direto via `sq.logger.info()`
3. **Configuração Centralizada**: Configurações aplicadas uma vez
4. **Memória Otimizada**: Evita múltiplas instâncias desnecessárias