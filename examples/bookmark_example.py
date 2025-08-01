#!/usr/bin/env python3
"""
Exemplo de uso de Job Bookmarks com Sequoia
Demonstra como detectar e usar job bookmarks para processamento incremental
"""

from sequoia.core import GlueClient


def main():
    """
    Exemplo de uso de Job Bookmarks
    """
    # Criar instância (Singleton)
    sq = GlueClient()

    # Inicializar job (opcional - apenas se usar job bookmarks)
    # sq.job_init()

    # Obter argumentos encapsulados
    database_name = sq.get_arg("database_name")
    table_name = sq.get_arg("table_name")

    sq.logger.info("=== Exemplo de Job Bookmarks ===")

    # Exemplo 1: Detectar se job bookmark está habilitado
    sq.logger.info("=== Exemplo 1: Detecção de Job Bookmark ===")

    is_bookmark_enabled = sq.is_job_bookmark_enabled()
    sq.logger.info(f"Job bookmark habilitado: {is_bookmark_enabled}")

    # Exemplo 2: Obter estado do bookmark
    sq.logger.info("=== Exemplo 2: Estado do Bookmark ===")

    bookmark_state = sq.get_job_bookmark_state()
    sq.logger.info(f"Estado do bookmark: {bookmark_state}")

    # Exemplo 3: Obter tabelas com bookmark
    sq.logger.info("=== Exemplo 3: Tabelas com Bookmark ===")

    # Nota: Método get_bookmark_tables() foi removido
    sq.logger.info("Método get_bookmark_tables() foi removido")

    # Exemplo 4: Leitura com bookmark
    sq.logger.info("=== Exemplo 4: Leitura com Bookmark ===")

    # Obter argumentos encapsulados
    database_name = sq.get_arg("database_name")
    table_name = sq.get_arg("table_name")

    # Leitura normal (sem bookmark)
    sq.logger.info("Lendo dados sem bookmark...")
    df_normal = sq.read_table(database_name, table_name)
    sq.logger.info(f"Dados lidos (normal): {df_normal.count()} registros")

    # Nota: Método read_table_with_bookmark() foi removido
    sq.logger.info("Método read_table_with_bookmark() foi removido")
    sq.logger.info("Use read_table() para leitura normal")

    # Exemplo 5: Leitura incremental com filtros
    sq.logger.info("=== Exemplo 5: Leitura Incremental ===")

    df_filtered = sq.read_table(
        database_name,
        table_name,
        columns=["id", "nome", "data_criacao"],
        where="data_criacao >= '2025-01-01'",
    )
    sq.logger.info(f"Dados filtrados: {df_filtered.count()} registros")

    # Exemplo 6: Processamento com bookmark
    sq.logger.info("=== Exemplo 6: Processamento com Bookmark ===")

    # Processar dados incrementais
    df_processed = df_normal.select("*").limit(1000)
    sq.logger.info(f"Dados processados: {df_processed.count()} registros")

    # Salvar resultados
    sq.write_table(
        df_processed, "resultados", "dados_incrementais", mode="append"
    )
    sq.logger.info("Resultados salvos com sucesso!")

    # Exemplo 7: Informações detalhadas do bookmark
    sq.logger.info("=== Exemplo 7: Informações Detalhadas ===")

    if is_bookmark_enabled:
        sq.logger.info("📋 Informações do Job Bookmark:")
        sq.logger.info(f"   - Job Name: {bookmark_state.get('job_name')}")
        sq.logger.info(f"   - Run ID: {bookmark_state.get('run_id')}")
        sq.logger.info(f"   - Enabled: {bookmark_state.get('enabled')}")

        # Mostrar algumas linhas dos dados
        sq.logger.info("Primeiras 3 linhas dos dados:")
        df_normal.show(3, truncate=False)
    else:
        sq.logger.info("ℹ️ Job bookmark não está habilitado para este job")

    sq.logger.info("Exemplo de job bookmarks executado com sucesso!")

    # Finalizar job (opcional - apenas se usar job bookmarks)
    # sq.job_commit()


if __name__ == "__main__":
    main()
