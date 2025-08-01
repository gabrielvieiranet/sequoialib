"""
Exemplo de operações com arquivos usando Sequoia
Demonstra leitura e escrita de diferentes formatos (Parquet, CSV, Excel)
"""

from sequoia.core import GlueClient


def main():
    """
    Exemplo de operações com arquivos
    """
    # Criar instância (Singleton)
    sq = GlueClient()

    # Exemplo 1: Leitura de diferentes formatos
    sq.logger.info("=== Exemplo 1: Leitura de Diferentes Formatos ===")

    # Ler Parquet (padrão)
    sq.logger.info("Lendo arquivo Parquet...")
    df_parquet = sq.read_file("s3://bucket/data.parquet")
    sq.logger.info(f"Dados Parquet: {df_parquet.count()} registros")

    # Ler CSV
    sq.logger.info("Lendo arquivo CSV...")
    df_csv = sq.read_file(
        "s3://bucket/data.csv",
        format_type="csv",
        options={"header": "true", "delimiter": ",", "encoding": "UTF-8"},
    )
    sq.logger.info(f"Dados CSV: {df_csv.count()} registros")

    # Ler Excel
    sq.logger.info("Lendo arquivo Excel...")
    df_excel = sq.read_file("s3://bucket/data.xlsx", format_type="excel")
    sq.logger.info(f"Dados Excel: {df_excel.count()} registros")

    # Ler Excel com planilha específica
    sq.logger.info("Lendo planilha específica do Excel...")
    df_excel_sheet = sq.read_file(
        "s3://bucket/data.xlsx",
        format_type="excel",
        options={"sheet_name": "Planilha1"},
    )
    sq.logger.info(f"Dados Excel (Planilha1): {df_excel_sheet.count()} registros")

    # Ler Excel com índice de planilha
    sq.logger.info("Lendo planilha por índice...")
    df_excel_index = sq.read_file(
        "s3://bucket/data.xlsx",
        format_type="excel",
        options={"sheet_name": 1},  # Segunda planilha
    )
    sq.logger.info(f"Dados Excel (índice 1): {df_excel_index.count()} registros")

    # Exemplo 2: Escrita de diferentes formatos
    sq.logger.info("=== Exemplo 2: Escrita de Diferentes Formatos ===")

    # Criar dados de exemplo
    sample_data = [
        {"id": 1, "nome": "João", "idade": 30},
        {"id": 2, "nome": "Maria", "idade": 25},
        {"id": 3, "nome": "Pedro", "idade": 35},
    ]

    df_sample = sq.createDataFrame(sample_data)

    # Salvar como CSV
    sq.logger.info("Salvando como CSV...")
    sq.write_file(
        df_sample,
        "s3://bucket/output/sample.csv",
        format_type="csv",
        mode="overwrite",
        options={"header": "true", "delimiter": ",", "encoding": "UTF-8"},
    )

    # Salvar como Parquet com compressão gzip
    sq.logger.info("Salvando como Parquet GZIP...")
    sq.write_file(
        df_sample,
        "s3://bucket/output/sample_gzip.parquet",
        format_type="parquet",
        mode="overwrite",
        options={"compression": "gzip"},
    )

    # Exemplo 3: Processamento de dados
    sq.logger.info("=== Exemplo 3: Processamento de Dados ===")

    # Ler dados
    df = sq.read_file("s3://bucket/input/data.parquet")
    sq.logger.info(f"Dados originais: {df.count()} registros")

    # Processar dados
    df_processed = df.filter("idade > 25").select("nome", "idade")
    sq.logger.info(f"Dados processados: {df_processed.count()} registros")

    # Salvar em múltiplos formatos
    sq.write_file(df_processed, "s3://bucket/output/processed.parquet")
    sq.write_file(
        df_processed,
        "s3://bucket/output/processed.csv",
        format_type="csv",
        options={"header": "true", "delimiter": ","},
    )

    # Exemplo 4: Configurações específicas
    sq.logger.info("=== Exemplo 4: Configurações Específicas ===")

    # CSV com delimitador diferente
    sq.write_file(
        df_sample,
        "s3://bucket/output/sample_semicolon.csv",
        format_type="csv",
        options={"header": "true", "delimiter": ";"},
    )

    # Parquet com compressão diferente
    sq.write_file(
        df_sample,
        "s3://bucket/output/sample_gzip.parquet",
        format_type="parquet",
        options={"compression": "gzip"},
    )

    # Exemplo 5: Leitura com opções customizadas
    sq.logger.info("=== Exemplo 5: Leitura com Opções Customizadas ===")

    # CSV sem cabeçalho
    df_csv_no_header = sq.read_file(
        "s3://bucket/data_no_header.csv",
        format_type="csv",
        options={"header": "false", "delimiter": ";", "inferSchema": "false"},
    )
    sq.logger.info(f"CSV sem cabeçalho: {df_csv_no_header.count()} registros")

    # Parquet com compressão gzip
    df_parquet_gzip = sq.read_file(
        "s3://bucket/data_gzip.parquet",
        format_type="parquet",
        options={"compression": "gzip", "mergeSchema": "true"},
    )
    sq.logger.info(f"Parquet GZIP: {df_parquet_gzip.count()} registros")

    # CSV com encoding específico
    df_csv_utf16 = sq.read_file(
        "s3://bucket/data_utf16.csv",
        format_type="csv",
        options={"header": "true", "delimiter": ",", "encoding": "UTF-16"},
    )
    sq.logger.info(f"CSV UTF-16: {df_csv_utf16.count()} registros")

    sq.logger.info("Exemplo de operações com arquivos executado com sucesso!")


if __name__ == "__main__":
    main()
