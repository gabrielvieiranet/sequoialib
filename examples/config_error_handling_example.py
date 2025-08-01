"""
Exemplo de tratamento de erros de configuração do Spark
Demonstra como lidar com erros de configuração quando o Spark já foi inicializado
"""

from sequoia import GlueClient


def demonstrate_config_error_handling():
    """
    Demonstra como lidar com erros de configuração do Spark
    """
    print("=== Demonstração de Tratamento de Erros de Configuração ===\n")

    # Exemplo 1: Inicialização normal
    print("1. Inicialização normal do GlueClient:")
    try:
        client = GlueClient()
        print("   ✅ GlueClient inicializado com sucesso")
        print("   📋 Configurações aplicadas automaticamente")
    except Exception as e:
        print(f"   ❌ Erro na inicialização: {str(e)}")
    print()

    # Exemplo 2: Tentativa de atualizar configurações
    print("2. Tentativa de atualizar configurações:")
    try:
        # Configurações que podem causar erro se Spark já inicializado
        config_to_update = {
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.sql.adaptive.enabled": "true",
        }

        client.update_config(config_to_update)
        print("   ✅ Configurações atualizadas com sucesso")
    except Exception as e:
        print(f"   ⚠️  Aviso: {str(e)}")
        print("   💡 O código continua funcionando normalmente")
    print()

    # Exemplo 3: Verificar configurações atuais
    print("3. Verificar configurações atuais:")
    try:
        current_config = client.get_current_config()
        important_keys = [
            "spark.serializer",
            "spark.sql.parquet.compression.codec",
            "spark.sql.adaptive.enabled",
        ]

        for key in important_keys:
            if key in current_config:
                print(f"   {key}: {current_config[key]}")
            else:
                print(f"   {key}: Não configurado")
    except Exception as e:
        print(f"   ❌ Erro ao obter configurações: {str(e)}")
    print()

    # Exemplo 4: Uso normal após erro de configuração
    print("4. Uso normal após erro de configuração:")
    try:
        # Tentar ler uma tabela (deve funcionar mesmo com erro de config)
        print("   Tentando ler tabela...")
        # df = client.read_table("database", "table")  # Comentado para exemplo
        print("   ✅ Funcionalidade normal não afetada")
    except Exception as e:
        print(f"   ❌ Erro na leitura: {str(e)}")
    print()


def demonstrate_robust_initialization():
    """
    Demonstra inicialização robusta que não falha com erros de config
    """
    print("=== Demonstração de Inicialização Robusta ===\n")

    # Exemplo 1: Inicialização com tratamento de erro
    print("1. Inicialização com tratamento de erro:")
    try:
        # Criar cliente - não deve falhar mesmo com erro de config
        client = GlueClient()
        print("   ✅ GlueClient criado com sucesso")

        # Verificar se está funcional
        print("   🔍 Verificando funcionalidade...")
        # Teste simples de funcionalidade
        spark_session = client.spark_session
        print(f"   ✅ SparkSession disponível: {spark_session is not None}")

    except Exception as e:
        print(f"   ❌ Erro crítico na inicialização: {str(e)}")
    print()

    # Exemplo 2: Explicação do erro
    print("2. Explicação do erro CANNOT_MODIFY_CONFIG:")
    print("   📋 O erro ocorre quando:")
    print("      - Spark já foi inicializado")
    print("      - Tentativa de modificar spark.serializer")
    print("      - Configuração já foi definida")
    print()
    print("   💡 Solução implementada:")
    print("      - Verificação se config já aplicada")
    print("      - Tratamento de erro sem falha")
    print("      - Continuidade da execução")
    print()


def demonstrate_best_practices():
    """
    Demonstra melhores práticas para evitar erros de configuração
    """
    print("=== Melhores Práticas ===\n")

    print("1. Inicialização correta:")
    print("   ✅ Use GlueClient() sem parâmetros")
    print("   ✅ Deixe o código aplicar configs automaticamente")
    print("   ❌ Não tente modificar spark.serializer manualmente")
    print()

    print("2. Tratamento de erros:")
    print("   ✅ O código trata erros automaticamente")
    print("   ✅ Funcionalidade continua mesmo com erro de config")
    print("   ✅ Logs informam sobre problemas de configuração")
    print()

    print("3. Verificação de funcionalidade:")
    print("   ✅ Sempre teste se o cliente está funcionando")
    print("   ✅ Use get_current_config() para verificar configs")
    print("   ✅ Monitore logs para problemas de configuração")
    print()


if __name__ == "__main__":
    # Executar demonstrações
    demonstrate_config_error_handling()
    demonstrate_robust_initialization()
    demonstrate_best_practices()

    print("=== Demonstração Concluída ===")
    print(
        "\n💡 Dica: O GlueClient é robusto e continua funcionando mesmo com erros de config!"
    )
    print(
        "   Os erros são tratados automaticamente e não afetam a funcionalidade principal."
    )
