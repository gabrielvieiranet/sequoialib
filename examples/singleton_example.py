"""
Exemplo de uso do padrão Singleton no Sequoia
Demonstra como usar a variável reduzida 'sq' e logs integrados
"""

from sequoia.core import GlueClient


def main():
    """
    Exemplo de uso do Singleton Sequoia
    """
    # Criar instância (Singleton - sempre a mesma)
    sq = GlueClient()

    # Logs integrados
    sq.logger.info("=== Exemplo de Singleton GlueClient ===")
    sq.logger.info("Iniciando demonstração do padrão Singleton")

    # Verificar se é a mesma instância
    sq2 = GlueClient()
    sq.logger.info(f"sq é sq2? {sq is sq2}")  # True - mesma instância

    # Usar logs integrados
    sq.logger.info("Testando logs integrados")
    sq.logger.warning("Este é um aviso")
    sq.logger.error("Este é um erro (simulado)")

    # Exemplo 1: Leitura de dados
    sq.logger.info("=== Exemplo 1: Leitura de Dados ===")

    try:
        # Ler dados (exemplo)
        df = sq.read_file("s3://bucket/data.parquet")
        sq.logger.info(f"Dados lidos: {df.count()} registros")

        # Processar dados
        df_processed = df.limit(100)
        sq.logger.info(f"Dados processados: {df_processed.count()} registros")

    except Exception as e:
        sq.logger.error(f"Erro ao processar dados: {str(e)}")

    # Exemplo 2: Job bookmarks
    sq.logger.info("=== Exemplo 2: Job Bookmarks ===")

    # Verificar se job bookmark está habilitado
    is_enabled = sq.is_job_bookmark_enabled()
    sq.logger.info(f"Job bookmark habilitado: {is_enabled}")

    if is_enabled:
        bookmark_state = sq.get_job_bookmark_state()
        sq.logger.info(f"Estado do bookmark: {bookmark_state}")

    # Exemplo 3: Configurações
    sq.logger.info("=== Exemplo 3: Configurações ===")

    # Obter configuração atual
    current_config = sq.get_current_config()
    sq.logger.info(f"Configurações ativas: {len(current_config)} parâmetros")

    # Atualizar configuração
    sq.update_config(
        {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        }
    )

    # Exemplo 4: Argumentos do job
    sq.logger.info("=== Exemplo 4: Argumentos do Job ===")

    try:
        # Obter argumentos
        args = sq.get_job_args(["JOB_NAME"])
        sq.logger.info(f"Argumentos obtidos: {list(args.keys())}")

        # Obter argumento específico
        job_name = sq.get_arg("JOB_NAME", default="meu_job")
        sq.logger.info(f"Nome do job: {job_name}")

    except Exception as e:
        sq.logger.warning(f"Erro ao obter argumentos: {str(e)}")

    # Exemplo 5: SQL
    sq.logger.info("=== Exemplo 5: Queries SQL ===")

    try:
        # Executar query SQL
        result = sq.sql("SELECT 1 as test")
        sq.logger.info(
            f"Query SQL executada com sucesso: {result.count()} registros"
        )

    except Exception as e:
        sq.logger.error(f"Erro na query SQL: {str(e)}")

    # Exemplo 6: Criação de DataFrame
    sq.logger.info("=== Exemplo 6: Criação de DataFrame ===")

    # Criar dados de exemplo
    sample_data = [
        {"id": 1, "nome": "João", "idade": 30},
        {"id": 2, "nome": "Maria", "idade": 25},
        {"id": 3, "nome": "Pedro", "idade": 35},
    ]

    df_sample = sq.createDataFrame(sample_data)
    sq.logger.info(f"DataFrame criado: {df_sample.count()} registros")

    # Exemplo 7: Otimização
    sq.logger.info("=== Exemplo 7: Otimização ===")

    df_optimized = sq.optimize_dataframe(df_sample, partition_column="idade")
    sq.logger.info(
        f"DataFrame otimizado com sucesso: {df_optimized.count()} registros"
    )

    # Exemplo 8: Informações da tabela
    sq.logger.info("=== Exemplo 8: Informações da Tabela ===")

    try:
        table_info = sq.get_table_info("database", "table")
        sq.logger.info(f"Informações da tabela: {table_info}")

    except Exception as e:
        sq.logger.warning(f"Erro ao obter informações da tabela: {str(e)}")

    sq.logger.info("=== Demonstração do Singleton concluída ===")
    sq.logger.info("✅ Todos os exemplos executados com sucesso!")


if __name__ == "__main__":
    main()
