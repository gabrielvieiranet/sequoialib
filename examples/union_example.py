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
    database_name = sq.get_arg("database_name")
    table1_name = sq.get_arg("table1_name", default="vendas_2024")
    table2_name = sq.get_arg("table2_name", default="vendas_2025")
    output_table = sq.get_arg("output_table", default="vendas_consolidadas")

    # Exemplo 1: Ler tabelas do catálogo
    # Ler primeira tabela
    sq.read_table(database_name, table1_name)

    # Ler segunda tabela
    sq.read_table(database_name, table2_name)

    # Exemplo 2: UNION via SQL
    # Query UNION simples
    union_query = f"""
    SELECT 
        id,
        nome,
        valor,
        data_venda,
        '2024' as ano_origem
    FROM {database_name}.{table1_name}
    
    UNION ALL
    
    SELECT 
        id,
        nome,
        valor,
        data_venda,
        '2025' as ano_origem
    FROM {database_name}.{table2_name}
    
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
    FROM {database_name}.{table1_name}
    WHERE valor > 100
    AND data_venda >= '2024-01-01'
    
    UNION ALL
    
    SELECT 
        id,
        nome,
        valor,
        data_venda,
        '2025' as ano_origem
    FROM {database_name}.{table2_name}
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
    FROM {database_name}.{table1_name}
    WHERE data_venda >= '2024-01-01'
    
    UNION ALL
    
    SELECT 
        '2025' as ano,
        COUNT(*) as total_vendas,
        SUM(valor) as valor_total,
        AVG(valor) as valor_medio
    FROM {database_name}.{table2_name}
    WHERE data_venda >= '2025-01-01'
    
    ORDER BY ano
    """

    sq.sql(union_agg_query)

    # Exemplo 5: Gravar resultado em nova tabela
    # Gravar UNION completo
    sq.write_table(df_union, database_name, output_table, mode="overwrite")

    # Gravar UNION filtrado
    sq.write_table(
        df_union_filtered,
        database_name,
        f"{output_table}_filtrado",
        mode="overwrite",
    )

    # Finalizar job (opcional - apenas se usar job bookmarks)
    # sq.job_commit()


if __name__ == "__main__":
    main()
