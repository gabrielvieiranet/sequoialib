"""
Exemplo simples de UNION entre tabelas do catálogo
Demonstra como ler duas tabelas, fazer UNION via SQL e gravar em nova tabela
"""

from sequoia.core import GlueClient


def main():
    """
    Exemplo de UNION entre tabelas do catálogo
    """
    # Criar instância (Singleton)
    sq = GlueClient()

    # Inicializar job (opcional - apenas se usar job bookmarks)
    # sq.job_init()

    # Obter argumentos encapsulados
    database = sq.get_arg("database")
    table1 = sq.get_arg("table1", default="vendas_2024")
    table2 = sq.get_arg("table2", default="vendas_2025")
    output_table = sq.get_arg("output_table", default="vendas_consolidadas")

    # Exemplo 1: Ler tabelas do catálogo
    # Ler primeira tabela
    sq.read_table(database, table1)

    # Ler segunda tabela
    sq.read_table(database, table2)

    # Exemplo 2: UNION via SQL
    # Query UNION simples
    union_query = f"""
    SELECT 
        id,
        nome,
        valor,
        data_venda,
        '2024' as ano_origem
    FROM {database}.{table1}
    
    UNION ALL
    
    SELECT 
        id,
        nome,
        valor,
        data_venda,
        '2025' as ano_origem
    FROM {database}.{table2}
    
    ORDER BY data_venda DESC
    """

    df_union = sq.sql(union_query)

    # Exemplo 3: UNION com filtros
    # Query UNION com filtros
    union_filtered_query = f"""
    SELECT 
        id,
        nome,
        valor,
        data_venda,
        '2024' as ano_origem
    FROM {database}.{table1}
    WHERE valor > 100
    AND data_venda >= '2024-01-01'
    
    UNION ALL
    
    SELECT 
        id,
        nome,
        valor,
        data_venda,
        '2025' as ano_origem
    FROM {database}.{table2}
    WHERE valor > 100
    AND data_venda >= '2025-01-01'
    
    ORDER BY valor DESC
    """

    df_union_filtered = sq.sql(union_filtered_query)

    # Exemplo 4: UNION com agregação
    # Query UNION com agregação
    union_agg_query = f"""
    SELECT 
        '2024' as ano,
        COUNT(*) as total_vendas,
        SUM(valor) as valor_total,
        AVG(valor) as valor_medio
    FROM {database}.{table1}
    WHERE data_venda >= '2024-01-01'
    
    UNION ALL
    
    SELECT 
        '2025' as ano,
        COUNT(*) as total_vendas,
        SUM(valor) as valor_total,
        AVG(valor) as valor_medio
    FROM {database}.{table2}
    WHERE data_venda >= '2025-01-01'
    
    ORDER BY ano
    """

    sq.sql(union_agg_query)

    # Exemplo 5: Gravar resultado em nova tabela
    # Gravar UNION completo
    sq.write_table(df_union, database, output_table, mode="overwrite")

    # Gravar UNION filtrado
    sq.write_table(
        df_union_filtered,
        database,
        f"{output_table}_filtrado",
        mode="overwrite",
    )

    # Finalizar job (opcional - apenas se usar job bookmarks)
    # sq.job_commit()


if __name__ == "__main__":
    main()
