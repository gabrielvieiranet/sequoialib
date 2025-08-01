#!/usr/bin/env python3
"""
Exemplo de uso do Sequoia com tabelas Iceberg
Demonstra leitura de tabelas Iceberg no AWS Glue
"""

from sequoia.core import GlueClient


def main():
    """
    Exemplo de uso com tabelas Iceberg
    """
    # Criar instância (Singleton)
    sq = GlueClient()

    # Inicializar job (opcional - apenas se usar job bookmarks)
    # sq.job_init()

    # Obter argumentos encapsulados
    database_name = sq.get_arg("database_name")
    table_name = sq.get_arg("table_name")
    output_path = sq.get_arg("output_path")

    sq.logger.info("=== Exemplo de Tabelas Iceberg ===")

    # Exemplo 1: Detecção automática de formato
    sq.logger.info("=== Exemplo 1: Detecção Automática ===")

    # Detectar formato da tabela
    table_format = sq.detect_table_format(database_name, table_name)
    sq.logger.info(f"Formato detectado: {table_format}")

    # Ler dados automaticamente
    df = sq.read_table_auto(database_name, table_name)
    sq.logger.info(f"Dados lidos: {df.count()} registros")

    # Exemplo 2: Leitura específica de Iceberg
    sq.logger.info("=== Exemplo 2: Leitura Específica Iceberg ===")

    if table_format == "iceberg":
        # Ler tabela Iceberg especificamente
        df_iceberg = sq.read_iceberg_table_from_catalog(
            database_name, table_name
        )
        sq.logger.info(f"Dados Iceberg: {df_iceberg.count()} registros")

        # Mostrar schema
        sq.logger.info("Schema da tabela Iceberg:")
        df_iceberg.printSchema()

    # Exemplo 3: Processamento de dados
    sq.logger.info("=== Exemplo 3: Processamento de Dados ===")

    # Filtrar dados
    df_filtered = df.filter("data_criacao >= '2025-01-01'")
    sq.logger.info(f"Dados filtrados: {df_filtered.count()} registros")

    # Selecionar colunas específicas
    df_selected = df_filtered.select("id", "nome", "data_criacao")
    sq.logger.info(f"Colunas selecionadas: {df_selected.columns}")

    # Exemplo 4: Salvar resultados
    sq.logger.info("=== Exemplo 4: Salvando Resultados ===")

    # Salvar como Parquet no S3
    sq.write_file(df_selected, f"{output_path}/resultados.parquet")
    sq.logger.info("Resultados salvos como Parquet")

    # Salvar como CSV
    sq.write_file(
        df_selected,
        f"{output_path}/resultados.csv",
        format_type="csv",
        options={"header": "true", "delimiter": ","},
    )
    sq.logger.info("Resultados salvos como CSV")

    # Exemplo 5: Informações da tabela
    sq.logger.info("=== Exemplo 5: Informações da Tabela ===")

    try:
        table_info = sq.get_table_info(database_name, table_name)
        sq.logger.info(f"Informações da tabela: {table_info}")
    except Exception as e:
        sq.logger.warning(f"Erro ao obter informações: {str(e)}")

    # Exemplo 6: Otimização de DataFrame
    sq.logger.info("=== Exemplo 6: Otimização ===")

    # Otimizar DataFrame
    df_optimized = sq.optimize_dataframe(
        df_selected, partition_column="data_criacao"
    )
    sq.logger.info(f"DataFrame otimizado: {df_optimized.count()} registros")

    # Exemplo 7: Queries SQL
    sq.logger.info("=== Exemplo 7: Queries SQL ===")

    # Executar query SQL
    df_sql = sq.sql(
        f"SELECT COUNT(*) as total FROM {database_name}.{table_name} "
        "WHERE data_criacao >= '2025-01-01'"
    )
    sq.logger.info("Query SQL executada com sucesso")

    # Mostrar resultado
    df_sql.show()

    sq.logger.info("=== Exemplo de Iceberg concluído ===")
    sq.logger.info("✅ Processamento concluído com sucesso!")

    # Finalizar job (opcional - apenas se usar job bookmarks)
    # sq.job_commit()


if __name__ == "__main__":
    main()
