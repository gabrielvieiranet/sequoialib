#!/usr/bin/env python3
"""
Exemplos de configurações customizadas do Spark usando Sequoia
Demonstra diferentes configurações para otimização de performance
"""

from sequoia.core import GlueClient


def main():
    """
    Exemplos de configurações customizadas
    """
    # Criar instância (Singleton)
    sq = GlueClient()

    # Exemplo 1: Configuração de Performance
    sq.logger.info("=== Exemplo 1: Configuração de Performance ===")

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

    # Exemplo 2: Configuração de Memória
    sq.logger.info("=== Exemplo 2: Configuração de Memória ===")

    memory_config = {
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "10m",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256m",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
    }

    sq.update_config(memory_config)
    sq.logger.info("Configurações de memória aplicadas")

    # Exemplo 3: Configuração de Parquet
    sq.logger.info("=== Exemplo 3: Configuração de Parquet ===")

    parquet_config = {
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.parquet.enableVectorizedReader": "true",
        "spark.sql.parquet.mergeSchema": "false",
        "spark.sql.parquet.filterPushdown": "true",
    }

    sq.update_config(parquet_config)
    sq.logger.info("Configurações de Parquet aplicadas")

    # Exemplo 4: Configuração de KryoSerializer
    sq.logger.info("=== Exemplo 4: Configuração de KryoSerializer ===")

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

    # Exemplo 5: Configuração de Iceberg
    sq.logger.info("=== Exemplo 5: Configuração de Iceberg ===")

    iceberg_config = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.glue_catalog.warehouse": "s3://your-warehouse-path/",
    }

    sq.update_config(iceberg_config)
    sq.logger.info("Configurações de Iceberg aplicadas")

    # Exemplo 6: Obter configuração atual
    sq.logger.info("=== Exemplo 6: Configuração Atual ===")

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

    # Exemplo 7: Configuração customizada para job específico
    sq.logger.info("=== Exemplo 7: Configuração Customizada ===")

    # Configuração específica para processamento de dados grandes
    large_data_config = {
        "spark.sql.shuffle.partitions": "500",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256m",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "50m",
    }

    sq.update_config(large_data_config)
    sq.logger.info("Configurações para dados grandes aplicadas")

    # Exemplo 8: Configuração para dados pequenos
    sq.logger.info("=== Exemplo 8: Configuração para Dados Pequenos ===")

    small_data_config = {
        "spark.sql.shuffle.partitions": "50",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64m",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "5m",
    }

    sq.update_config(small_data_config)
    sq.logger.info("Configurações para dados pequenos aplicadas")

    sq.logger.info("=== Todos os exemplos de configuração executados ===")
    sq.logger.info("✅ Configurações aplicadas com sucesso!")


if __name__ == "__main__":
    main()
