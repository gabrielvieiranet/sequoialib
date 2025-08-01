#!/usr/bin/env python3
"""
Exemplo principal de uso do Sequoia
Demonstra diferentes configurações e funcionalidades
"""

from sequoia.core import GlueClient


def main():
    """
    Exemplo principal de uso do Sequoia
    """
    # Criar instância (Singleton)
    sq = GlueClient()

    sq.logger.info("=== Exemplo Principal do Sequoia ===")

    # Exemplo 1: Configuração padrão
    sq.logger.info("=== Exemplo 1: Configuração Padrão ===")

    # Criar instância com configuração padrão
    sq_default = GlueClient()
    sq.logger.info("Instância padrão criada")

    # Exemplo 2: Configuração de performance
    sq.logger.info("=== Exemplo 2: Configuração de Performance ===")

    # Configurações para melhor performance
    performance_config = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
    }

    sq.update_config(performance_config)
    sq.logger.info("Configurações de performance aplicadas")

    # Exemplo 3: Configuração de memória
    sq.logger.info("=== Exemplo 3: Configuração de Memória ===")

    memory_config = {
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "10m",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256m",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
    }

    sq.update_config(memory_config)
    sq.logger.info("Configurações de memória aplicadas")

    # Exemplo 4: Configuração de Parquet
    sq.logger.info("=== Exemplo 4: Configuração de Parquet ===")

    parquet_config = {
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.parquet.enableVectorizedReader": "true",
        "spark.sql.parquet.mergeSchema": "false",
        "spark.sql.parquet.filterPushdown": "true",
    }

    sq.update_config(parquet_config)
    sq.logger.info("Configurações de Parquet aplicadas")

    # Exemplo 5: Configuração de KryoSerializer
    sq.logger.info("=== Exemplo 5: Configuração de KryoSerializer ===")

    kryo_config = {
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrationRequired": "false",
        "spark.kryo.registrator": "org.apache.spark.serializer.KryoRegistrator",
        "spark.kryo.unsafe": "true",
        "spark.kryoserializer.buffer.max": "2047m",
        "spark.kryoserializer.buffer": "64k",
    }

    sq.update_config(kryo_config)
    sq.logger.info("Configurações de KryoSerializer aplicadas")

    # Exemplo 6: Configuração de Iceberg
    sq.logger.info("=== Exemplo 6: Configuração de Iceberg ===")

    iceberg_config = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.glue_catalog.warehouse": "s3://your-warehouse-path/",
    }

    sq.update_config(iceberg_config)
    sq.logger.info("Configurações de Iceberg aplicadas")

    # Exemplo 7: Obter configuração atual
    sq.logger.info("=== Exemplo 7: Configuração Atual ===")

    current_config = sq.get_current_config()
    sq.logger.info(f"Total de configurações ativas: {len(current_config)}")

    # Mostrar algumas configurações importantes
    important_keys = [
        "spark.sql.adaptive.enabled",
        "spark.serializer",
        "spark.sql.parquet.compression.codec",
        "spark.sql.extensions",
    ]

    for key in important_keys:
        if key in current_config:
            sq.logger.info(f"{key}: {current_config[key]}")

    # Exemplo 8: Verificar se é Singleton
    sq.logger.info("=== Exemplo 8: Verificação Singleton ===")

    sq2 = GlueClient()
    sq.logger.info(f"sq é sq2? {sq is sq2}")  # True - mesma instância

    # Exemplo 9: Logs integrados
    sq.logger.info("=== Exemplo 9: Logs Integrados ===")

    sq.logger.info("Testando logs integrados")
    sq.logger.warning("Este é um aviso")
    sq.logger.error("Este é um erro (simulado)")

    # Exemplo 10: Argumentos do job
    sq.logger.info("=== Exemplo 10: Argumentos do Job ===")

    try:
        # Obter argumentos
        args = sq.get_job_args(["JOB_NAME"])
        sq.logger.info(f"Argumentos obtidos: {list(args.keys())}")

        # Obter argumento específico
        job_name = sq.get_arg("JOB_NAME", default="meu_job")
        sq.logger.info(f"Nome do job: {job_name}")

    except Exception as e:
        sq.logger.warning(f"Erro ao obter argumentos: {str(e)}")

    # Exemplo 11: Criação de DataFrame
    sq.logger.info("=== Exemplo 11: Criação de DataFrame ===")

    # Criar dados de exemplo
    sample_data = [
        {"id": 1, "nome": "João", "idade": 30},
        {"id": 2, "nome": "Maria", "idade": 25},
        {"id": 3, "nome": "Pedro", "idade": 35},
    ]

    df_sample = sq.createDataFrame(sample_data)
    sq.logger.info(f"DataFrame criado: {df_sample.count()} registros")

    # Exemplo 12: SQL
    sq.logger.info("=== Exemplo 12: Queries SQL ===")

    try:
        # Executar query SQL
        result = sq.sql("SELECT 1 as test")
        sq.logger.info(f"Query SQL executada: {result.count()} registros")

    except Exception as e:
        sq.logger.error(f"Erro na query SQL: {str(e)}")

    sq.logger.info("=== Exemplo principal concluído ===")
    sq.logger.info("✅ Todos os exemplos executados com sucesso!")


if __name__ == "__main__":
    main()
