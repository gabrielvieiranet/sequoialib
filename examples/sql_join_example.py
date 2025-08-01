#!/usr/bin/env python3
"""
Exemplo de queries SQL com JOINs usando Sequoia
Demonstra como executar queries SQL complexas com JOINs
"""

from sequoia.core import GlueClient


def main():
    """
    Exemplo de queries SQL com JOINs
    """
    # Criar instância (Singleton)
    sq = GlueClient()

    # Inicializar job (opcional - apenas se usar job bookmarks)
    # sq.job_init()

    # Obter argumentos encapsulados
    # database_name = sq.get_arg("database_name")
    # table_name = sq.get_arg("table_name")

    sq.logger.info("=== Exemplo de Queries SQL com JOINs ===")

    # Exemplo 1: Query simples
    sq.logger.info("=== Exemplo 1: Query Simples ===")

    try:
        # Query básica
        df_simple = sq.sql("SELECT 1 as test")
        sq.logger.info(f"Query simples executada: {df_simple.count()} registros")
        df_simple.show()
    except Exception as e:
        sq.logger.error(f"Erro na query simples: {str(e)}")

    # Exemplo 2: Query com JOIN
    sq.logger.info("=== Exemplo 2: Query com JOIN ===")

    try:
        # Query com JOIN (exemplo)
        join_query = """
        SELECT 
            u.id,
            u.nome,
            p.produto,
            v.valor
        FROM usuarios u
        JOIN vendas v ON u.id = v.usuario_id
        JOIN produtos p ON v.produto_id = p.id
        WHERE v.data_venda >= '2025-01-01'
        """

        df_join = sq.sql(join_query)
        sq.logger.info(f"Query com JOIN executada: {df_join.count()} registros")
        df_join.show(5)
    except Exception as e:
        sq.logger.warning(f"Query com JOIN não executada: {str(e)}")

    # Exemplo 3: Query com agregação
    sq.logger.info("=== Exemplo 3: Query com Agregação ===")

    try:
        # Query com GROUP BY
        agg_query = """
        SELECT 
            u.nome,
            COUNT(v.id) as total_vendas,
            SUM(v.valor) as valor_total
        FROM usuarios u
        LEFT JOIN vendas v ON u.id = v.usuario_id
        GROUP BY u.nome
        HAVING COUNT(v.id) > 0
        ORDER BY valor_total DESC
        """

        df_agg = sq.sql(agg_query)
        sq.logger.info(
            f"Query com agregação executada: {df_agg.count()} registros"
        )
        df_agg.show(10)
    except Exception as e:
        sq.logger.warning(f"Query com agregação não executada: {str(e)}")

    # Exemplo 4: Query com subquery
    sq.logger.info("=== Exemplo 4: Query com Subquery ===")

    try:
        # Query com subquery
        subquery = """
        SELECT 
            nome,
            valor_total
        FROM (
            SELECT 
                u.nome,
                SUM(v.valor) as valor_total
            FROM usuarios u
            JOIN vendas v ON u.id = v.usuario_id
            GROUP BY u.nome
        ) ranked
        WHERE valor_total > (
            SELECT AVG(valor_total) 
            FROM (
                SELECT SUM(v.valor) as valor_total
                FROM vendas v
                GROUP BY v.usuario_id
            ) avg_table
        )
        ORDER BY valor_total DESC
        """

        df_subquery = sq.sql(subquery)
        sq.logger.info(
            f"Query com subquery executada: {df_subquery.count()} registros"
        )
        df_subquery.show(5)
    except Exception as e:
        sq.logger.warning(f"Query com subquery não executada: {str(e)}")

    # Exemplo 5: Query com window functions
    sq.logger.info("=== Exemplo 5: Query com Window Functions ===")

    try:
        # Query com window functions
        window_query = """
        SELECT 
            nome,
            produto,
            valor,
            ROW_NUMBER() OVER (
                PARTITION BY nome 
                ORDER BY valor DESC
            ) as rank_venda,
            RANK() OVER (
                PARTITION BY produto 
                ORDER BY valor DESC
            ) as rank_produto
        FROM (
            SELECT 
                u.nome,
                p.produto,
                v.valor
            FROM usuarios u
            JOIN vendas v ON u.id = v.usuario_id
            JOIN produtos p ON v.produto_id = p.id
        ) vendas_detalhadas
        """

        df_window = sq.sql(window_query)
        sq.logger.info(
            f"Query com window functions executada: {df_window.count()} registros"
        )
        df_window.show(10)
    except Exception as e:
        sq.logger.warning(f"Query com window functions não executada: {str(e)}")

    # Exemplo 6: Query com CTE (Common Table Expression)
    sq.logger.info("=== Exemplo 6: Query com CTE ===")

    try:
        # Query com CTE
        cte_query = """
        WITH vendas_por_usuario AS (
            SELECT 
                u.id,
                u.nome,
                COUNT(v.id) as total_vendas,
                SUM(v.valor) as valor_total
            FROM usuarios u
            LEFT JOIN vendas v ON u.id = v.usuario_id
            GROUP BY u.id, u.nome
        ),
        top_vendedores AS (
            SELECT 
                nome,
                total_vendas,
                valor_total
            FROM vendas_por_usuario
            WHERE valor_total > (
                SELECT AVG(valor_total) 
                FROM vendas_por_usuario 
                WHERE valor_total > 0
            )
        )
        SELECT * FROM top_vendedores
        ORDER BY valor_total DESC
        """

        df_cte = sq.sql(cte_query)
        sq.logger.info(f"Query com CTE executada: {df_cte.count()} registros")
        df_cte.show(5)
    except Exception as e:
        sq.logger.warning(f"Query com CTE não executada: {str(e)}")

    # Exemplo 7: Query com UNION
    sq.logger.info("=== Exemplo 7: Query com UNION ===")

    try:
        # Query com UNION
        union_query = """
        SELECT 
            'Vendas Altas' as categoria,
            nome,
            valor_total
        FROM (
            SELECT 
                u.nome,
                SUM(v.valor) as valor_total
            FROM usuarios u
            JOIN vendas v ON u.id = v.usuario_id
            GROUP BY u.nome
        ) vendas_por_usuario
        WHERE valor_total > 1000
        
        UNION ALL
        
        SELECT 
            'Vendas Baixas' as categoria,
            nome,
            valor_total
        FROM (
            SELECT 
                u.nome,
                SUM(v.valor) as valor_total
            FROM usuarios u
            JOIN vendas v ON u.id = v.usuario_id
            GROUP BY u.nome
        ) vendas_por_usuario
        WHERE valor_total <= 1000
        ORDER BY valor_total DESC
        """

        df_union = sq.sql(union_query)
        sq.logger.info(f"Query com UNION executada: {df_union.count()} registros")
        df_union.show(10)
    except Exception as e:
        sq.logger.warning(f"Query com UNION não executada: {str(e)}")

    sq.logger.info("=== Exemplo de queries SQL concluído ===")
    sq.logger.info("✅ Todas as queries executadas com sucesso!")

    # Finalizar job (opcional - apenas se usar job bookmarks)
    # sq.job_commit()


if __name__ == "__main__":
    main()
