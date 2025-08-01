#!/usr/bin/env python3
"""
Exemplo simples de detecção automática de formato de tabela
Demonstra como o Sequoia detecta automaticamente se é Parquet ou Iceberg
"""

from sequoia.core import GlueClient


def main():
    """
    Exemplo de detecção automática de formato
    """
    # Criar instância (Singleton)
    sq = GlueClient()

    # Inicializar job (opcional - apenas se usar job bookmarks)
    # sq.job_init()

    # Obter argumentos encapsulados
    database_name = sq.get_arg("database_name")
    table_name = sq.get_arg("table_name")

    sq.logger.info("=== Exemplo de Detecção Automática ===")

    # Exemplo 1: Detecção automática
    sq.logger.info("=== Exemplo 1: Detecção Automática ===")

    # Detectar formato da tabela
    table_format = sq.detect_table_format(database_name, table_name)
    sq.logger.info(f"Formato detectado: {table_format}")

    # Ler dados automaticamente (detecta formato e lê)
    df = sq.read_table_auto(database_name, table_name)
    sq.logger.info(f"Dados lidos automaticamente: {df.count()} registros")

    # Exemplo 2: Leitura específica por formato
    sq.logger.info("=== Exemplo 2: Leitura Específica ===")

    if table_format == "iceberg":
        # Ler como Iceberg
        df_iceberg = sq.read_iceberg_table_from_catalog(
            database_name, table_name
        )
        sq.logger.info(f"Dados Iceberg: {df_iceberg.count()} registros")
    else:
        # Ler como Parquet
        df_parquet = sq.read_table_from_catalog(database_name, table_name)
        sq.logger.info(f"Dados Parquet: {df_parquet.count()} registros")

    # Exemplo 3: Processamento de dados
    sq.logger.info("=== Exemplo 3: Processamento ===")

    # Filtrar dados
    df_filtered = df.filter("data_criacao >= '2025-01-01'")
    sq.logger.info(f"Dados filtrados: {df_filtered.count()} registros")

    # Mostrar schema
    sq.logger.info("Schema dos dados:")
    df_filtered.printSchema()

    # Exemplo 4: Informações da tabela
    sq.logger.info("=== Exemplo 4: Informações ===")

    try:
        table_info = sq.get_table_info(database_name, table_name)
        sq.logger.info(f"Informações da tabela: {table_info}")
    except Exception as e:
        sq.logger.warning(f"Erro ao obter informações: {str(e)}")

    sq.logger.info("=== Exemplo de detecção automática concluído ===")
    sq.logger.info("✅ Processamento concluído com sucesso!")

    # Finalizar job (opcional - apenas se usar job bookmarks)
    # sq.job_commit()


if __name__ == "__main__":
    main()
