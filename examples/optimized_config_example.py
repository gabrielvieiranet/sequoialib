"""
Exemplo de configuração otimizada do Spark
Demonstra como usar a nova estratégia de configuração via SparkConf
"""

from sequoia import GlueClient


def demonstrate_optimized_config():
    """
    Demonstra a nova estratégia de configuração otimizada
    """
    print("=== Demonstração de Configuração Otimizada ===\n")

    # Exemplo 1: Inicialização com configurações padrão
    print("1. Inicialização com configurações padrão:")
    try:
        client = GlueClient()
        print("   ✅ GlueClient inicializado com configurações otimizadas")
        print("   📋 KryoSerializer, Parquet e performance otimizados")
    except Exception as e:
        print(f"   ❌ Erro na inicialização: {str(e)}")
    print()

    # Exemplo 2: Inicialização com configurações customizadas
    print("2. Inicialização com configurações customizadas:")
    try:
        custom_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
        }

        client_custom = GlueClient(spark_config=custom_config)
        print("   ✅ GlueClient inicializado com configs customizadas")
        print("   📋 Configurações adaptativas habilitadas")
    except Exception as e:
        print(f"   ❌ Erro na inicialização: {str(e)}")
    print()

    # Exemplo 3: Verificar configurações aplicadas
    print("3. Verificar configurações aplicadas:")
    try:
        current_config = client.get_current_config()
        important_keys = [
            "spark.serializer",
            "spark.sql.parquet.compression.codec",
            "spark.sql.adaptive.enabled",
            "spark.kryo.registrationRequired",
        ]

        for key in important_keys:
            if key in current_config:
                print(f"   {key}: {current_config[key]}")
            else:
                print(f"   {key}: Não configurado")
    except Exception as e:
        print(f"   ❌ Erro ao verificar configs: {str(e)}")
    print()

    # Exemplo 4: Comparação de performance
    print("4. Comparação de performance:")
    print("   - Configuração via SparkConf: Aplicada na inicialização")
    print("   - Configuração via SparkSession: Pode falhar se já inicializado")
    print(
        "   - Nova estratégia: Sempre funciona, sem erros CANNOT_MODIFY_CONFIG"
    )
    print()

    # Exemplo 5: Tratamento de ambiente
    print("5. Tratamento de ambiente:")
    print("   - AWS Glue: GlueContext criado normalmente")
    print("   - Ambiente local: Fallback para SparkSession")
    print("   - Funciona em ambos os ambientes")
    print()


def demonstrate_config_strategy():
    """
    Demonstra a estratégia de configuração
    """
    print("=== Estratégia de Configuração ===\n")

    print("1. Configurações Padrão Unificadas:")
    print("   📋 KryoSerializer:")
    print("      - spark.serializer: KryoSerializer")
    print("      - spark.kryo.registrationRequired: false")
    print("      - spark.kryoserializer.buffer.max: 2047m")
    print()
    print("   📋 Parquet Otimizado:")
    print("      - spark.sql.parquet.compression.codec: snappy")
    print("      - spark.sql.parquet.filterPushdown: true")
    print("      - spark.sql.parquet.block.size: 128MB")
    print()
    print("   📋 Performance:")
    print("      - spark.sql.adaptive.enabled: true")
    print("      - spark.sql.adaptive.coalescePartitions.enabled: true")
    print("      - spark.sql.adaptive.skewJoin.enabled: true")
    print()

    print("2. Vantagens da Nova Estratégia:")
    print("   ✅ Configurações aplicadas na inicialização")
    print("   ✅ Sem erros CANNOT_MODIFY_CONFIG")
    print("   ✅ Configurações unificadas em um local")
    print("   ✅ Fallback para SparkContext padrão se falhar")
    print("   ✅ Configurações customizadas suportadas")
    print()


def demonstrate_usage_patterns():
    """
    Demonstra padrões de uso da nova configuração
    """
    print("=== Padrões de Uso ===\n")

    print("1. Uso Básico (Recomendado):")
    print("   ```python")
    print("   # Configurações otimizadas aplicadas automaticamente")
    print("   client = GlueClient()")
    print("   df = client.read_table('database', 'table')")
    print("   ```")
    print()

    print("2. Uso com Configurações Customizadas:")
    print("   ```python")
    print("   custom_config = {")
    print("       'spark.sql.adaptive.enabled': 'true',")
    print("       'spark.sql.adaptive.coalescePartitions.enabled': 'true',")
    print("   }")
    print("   client = GlueClient(spark_config=custom_config)")
    print("   ```")
    print()

    print("3. Uso com Contextos Existentes:")
    print("   ```python")
    print("   # Se você já tem contextos configurados")
    print("   client = GlueClient(")
    print("       spark_context=existing_spark_context,")
    print("       glue_context=existing_glue_context")
    print("   )")
    print("   ```")
    print()


if __name__ == "__main__":
    # Executar demonstrações
    demonstrate_optimized_config()
    demonstrate_config_strategy()
    demonstrate_usage_patterns()

    print("=== Demonstração Concluída ===")
    print(
        "\n💡 Dica: Use GlueClient() sem parâmetros para configurações otimizadas!"
    )
    print(
        "   As configurações são aplicadas automaticamente na inicialização."
    )
