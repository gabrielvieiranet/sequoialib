"""
Utilitários para operações Spark no AWS Glue
"""

import sys
from typing import Any, Dict

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession

from sequoia.logger import Logger


class GlueClient:
    """
    Cliente principal para operações Spark no AWS Glue
    Implementa padrão Singleton para instância única
    """

    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        """
        Implementa padrão Singleton
        """
        if cls._instance is None:
            cls._instance = super(GlueClient, cls).__new__(cls)
        return cls._instance

    def __init__(
        self,
        spark_context: SparkContext = None,
        glue_context: GlueContext = None,
        spark_session: SparkSession = None,
        spark_config: Dict[str, str] = None,
    ):
        """
        Inicializa a classe GlueClient (Singleton)

        Args:
            spark_context: Contexto Spark (opcional)
            glue_context: Contexto Glue (opcional)
            spark_session: Sessão Spark (opcional)
            spark_config: Configurações customizadas do Spark (opcional)
        """
        # Evitar reinicialização se já foi inicializado
        if self._initialized:
            return

        # Logger integrado (inicializar primeiro)
        self.logger = Logger("GlueClient")

        # Se não fornecido, criar automaticamente com configurações otimizadas
        if spark_context is None:
            spark_context = self._create_optimized_spark_context(spark_config)

        if glue_context is None:
            try:
                glue_context = GlueContext(spark_context)
                self.logger.info("GlueContext criado com sucesso")
            except Exception as e:
                self.logger.warning(f"Erro ao criar GlueContext: {str(e)}")
                self.logger.info("Usando SparkSession diretamente")
                glue_context = None

        if spark_session is None:
            if glue_context is not None:
                spark_session = glue_context.spark_session
            else:
                # Criar SparkSession diretamente se GlueContext falhar
                from pyspark.sql import SparkSession

                spark_session = SparkSession.builder.getOrCreate()

        # Encapsular contextos
        self._spark_context = spark_context
        self._glue_context = glue_context
        self._spark_session = spark_session

        # Propriedades públicas para acesso direto quando necessário
        self.spark_context = spark_context
        self.glue_context = glue_context
        self.spark_session = spark_session

        # Job instance (para job bookmarks)
        self._job = None

        # Job arguments (obtidos automaticamente)
        self._args = None

        # Marcar como inicializado
        self._initialized = True

    def _get_default_spark_config(self) -> Dict[str, str]:
        """
        Retorna configurações padrão unificadas do Spark

        Returns:
            Dicionário com configurações padrão
        """
        return {
            # Configurações KryoSerializer
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.kryo.registrationRequired": "false",
            "spark.kryo.registrator": "org.apache.spark.serializer.KryoRegistrator",
            "spark.kryo.unsafe": "true",
            "spark.kryoserializer.buffer.max": "2047m",
            "spark.kryoserializer.buffer": "64k",
            # Configurações Parquet
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.sql.parquet.mergeSchema": "false",
            "spark.sql.parquet.filterPushdown": "true",
            "spark.sql.parquet.columnarReaderBatchSize": "4096",
            "spark.sql.parquet.block.size": "134217728",  # 128MB
            "spark.sql.parquet.page.size": "1048576",  # 1MB
            "spark.parquet.enableVectorizedReader": "false",
            "spark.parquet.writeLegacyFormat": "true",
            # Configurações de performance
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            # Configurações de memória e execução
            "spark.driver.maxResultSize": "4g",
            "spark.shuffle.io.maxRetries": "10",
            "spark.shuffle.io.retryWait": "60s",
            "spark.reducer.maxReqsInFlight": "1",
            "spark.executor.memoryOverhead": "2g",
            "spark.memory.fraction": "0.6",
            # Configurações de particionamento
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            # Configurações de compatibilidade de data/hora
            "spark.sql.legacy.parquet.int96RebaseModeInRead": "CORRECTED",
            "spark.sql.legacy.parquet.int96RebaseModeInWrite": "CORRECTED",
            "spark.sql.legacy.parquet.datetimeRebaseModeInRead": "CORRECTED",
            "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "CORRECTED",
            # Configurações de JVM
            "spark.executor.extraJavaOptions": "-XX:+UseG1GC",
            # Configurações de timezone
            "spark.sql.session.timeZone": "America/Sao_Paulo",
            # Configurações de checkpoint
            "spark.checkpoint.compress": "true",
        }

    def _create_optimized_spark_context(
        self, custom_config: Dict[str, str] = None
    ) -> SparkContext:
        """
        Cria SparkContext com configurações otimizadas

        Args:
            custom_config: Configurações customizadas (opcional)

        Returns:
            SparkContext otimizado
        """
        try:
            from pyspark import SparkConf

            # Obter configurações padrão
            spark_conf = SparkConf()
            default_config = self._get_default_spark_config()

            # Aplicar configurações padrão
            for key, value in default_config.items():
                spark_conf.set(key, value)

            # Aplicar configurações customizadas se fornecidas
            if custom_config:
                for key, value in custom_config.items():
                    spark_conf.set(key, value)
                    self.logger.info(
                        f"Configuração customizada aplicada: {key} = {value}"
                    )

            # Criar SparkContext com configurações
            spark_context = SparkContext(conf=spark_conf)

            self.logger.info(
                "SparkContext criado com configurações otimizadas"
            )
            return spark_context

        except Exception as e:
            self.logger.error(
                f"Erro ao criar SparkContext otimizado: {str(e)}"
            )
            # Fallback para SparkContext padrão
            self.logger.warning("Usando SparkContext padrão")
            try:
                return SparkContext()
            except Exception as fallback_error:
                self.logger.error(f"Erro no fallback: {str(fallback_error)}")
                # Último recurso: tentar sem configurações
                return SparkContext.getOrCreate()

    def get_job_args(self, required_args: list = None) -> Dict[str, str]:
        """
        Obtém argumentos do job automaticamente

        Args:
            required_args: Lista de argumentos obrigatórios (opcional)

        Returns:
            Dicionário com argumentos do job
        """
        try:
            if self._args is None:
                from awsglue.utils import getResolvedOptions

                # Argumentos padrão se não especificados
                if required_args is None:
                    required_args = ["JOB_NAME"]

                self._args = getResolvedOptions(sys.argv, required_args)
                self.logger.info(
                    f"Argumentos do job obtidos: {list(self._args.keys())}"
                )

            return self._args

        except Exception as e:
            self.logger.error(f"Erro ao obter argumentos do job: {str(e)}")
            raise

    def get_arg(self, key: str, default: str = None) -> str:
        """
        Obtém um argumento específico do job

        Args:
            key: Nome do argumento
            default: Valor padrão se não encontrado

        Returns:
            Valor do argumento
        """
        try:
            args = self.get_job_args()
            return args.get(key, default)

        except Exception as e:
            self.logger.error(f"Erro ao obter argumento '{key}': {str(e)}")
            return default

    def job_init(self, job_name: str = None, required_args: list = None):
        """
        Inicializa o job Glue (necessário para job bookmarks)

        Args:
            job_name: Nome do job (opcional)
            required_args: Lista de argumentos obrigatórios (opcional)
        """
        try:
            from awsglue.job import Job

            # Obter argumentos primeiro
            self.get_job_args(required_args)

            # Inicializar job
            if job_name is None:
                job_name = self.get_arg("JOB_NAME", "default_job")

            self._job = Job(glue_context=self._glue_context)
            self._job.init(job_name, self._args)
            self.logger.info(f"Job inicializado: {job_name}")

        except Exception as e:
            self.logger.error(f"Erro ao inicializar job: {str(e)}")
            raise

    def job_commit(self):
        """
        Finaliza o job Glue (necessário para job bookmarks)
        """
        try:
            if self._job is not None:
                self._job.commit()
                self.logger.info("Job finalizado com sucesso")
            else:
                self.logger.warning("Job não foi inicializado")

        except Exception as e:
            self.logger.error(f"Erro ao finalizar job: {str(e)}")
            raise

    def get_current_config(self) -> Dict[str, str]:
        """
        Obtém configurações atuais do Spark

        Returns:
            Dicionário com configurações atuais
        """
        try:
            current_config = {}
            for key in self._spark_session.conf.getAll():
                try:
                    current_config[key[0]] = key[1]
                except Exception:
                    pass
            return current_config

        except Exception as e:
            self.logger.error(f"Erro ao obter configurações: {str(e)}")
            raise

    def update_config(self, new_config: Dict[str, str]):
        """
        Atualiza configurações do Spark dinamicamente
        (Nota: Configurações críticas devem ser aplicadas na inicialização)

        Args:
            new_config: Novas configurações para aplicar
        """
        try:
            for key, value in new_config.items():
                try:
                    self._spark_session.conf.set(key, value)
                    self.logger.info(
                        f"Configuração atualizada: {key} = {value}"
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Não foi possível configurar {key}: {str(e)}"
                    )
        except Exception as e:
            self.logger.error(f"Erro ao atualizar configuração: {str(e)}")
            self.logger.warning("Continuando sem atualizar configurações")

    def read_table_from_catalog(
        self,
        database_name: str,
        table_name: str,
        columns: list = None,
        where: str = None,
    ) -> DataFrame:
        """
        Lê uma tabela do Glue Catalog usando Spark DataFrame

        Args:
            database_name: Nome do banco de dados
            table_name: Nome da tabela
            columns: Lista de colunas para selecionar (opcional)
            where: Condição WHERE para filtrar dados (opcional)

        Returns:
            DataFrame com os dados da tabela
        """
        try:
            # Construir query SQL
            if columns:
                columns_str = ", ".join(columns)
                query = (
                    f"SELECT {columns_str} FROM {database_name}.{table_name}"
                )
            else:
                query = f"SELECT * FROM {database_name}.{table_name}"

            # Adicionar condição WHERE se fornecida
            if where:
                query += f" WHERE {where}"

            self.logger.info(f"Executando query: {query}")

            df = self._spark_session.sql(query)
            return df

        except Exception as e:
            self.logger.error(
                f"Erro ao ler tabela {database_name}.{table_name}: {str(e)}"
            )
            raise

    def read_iceberg_table_from_catalog(
        self,
        database_name: str,
        table_name: str,
        columns: list = None,
        where: str = None,
    ) -> DataFrame:
        """
        Lê uma tabela Iceberg do Glue Catalog usando Spark DataFrame

        Args:
            database_name: Nome do banco de dados
            table_name: Nome da tabela Iceberg
            columns: Lista de colunas para selecionar (opcional)
            where: Condição WHERE para filtrar dados (opcional)

        Returns:
            DataFrame com os dados da tabela Iceberg
        """
        try:
            # Verificar se o warehouse do Iceberg foi configurado
            warehouse_config = self._spark_session.conf.get(
                "spark.sql.catalog.glue_catalog.warehouse", None
            )

            if (
                not warehouse_config
                or warehouse_config == "s3://your-warehouse-path/"
            ):
                self.logger.warning(
                    "⚠️  ATENÇÃO: Warehouse do Iceberg não configurado! "
                    "Configure 'spark.sql.catalog.glue_catalog.warehouse' "
                    "com um caminho S3 válido para evitar problemas."
                )

            # Construir query SQL para Iceberg
            if columns:
                columns_str = ", ".join(columns)
                query = f"SELECT {columns_str} FROM glue_catalog.{database_name}.{table_name}"
            else:
                query = (
                    f"SELECT * FROM glue_catalog.{database_name}.{table_name}"
                )

            # Adicionar condição WHERE se fornecida
            if where:
                query += f" WHERE {where}"

            self.logger.info(f"Executando query Iceberg: {query}")

            df = self._spark_session.sql(query)
            return df

        except Exception as e:
            self.logger.error(
                f"Erro ao ler tabela Iceberg {database_name}.{table_name}: {str(e)}"
            )
            raise

    def detect_table_format(self, database_name: str, table_name: str) -> str:
        """
        Detecta automaticamente o formato da tabela usando metadados do Glue Catalog
        (muito mais eficiente que consultar dados)

        Args:
            database_name: Nome do banco de dados
            table_name: Nome da tabela

        Returns:
            Formato detectado ('iceberg', 'parquet', 'csv', etc.)
        """
        try:

            # Importar boto3 para acessar Glue Catalog
            try:
                import boto3
                from botocore.exceptions import ClientError
            except ImportError:
                self.logger.error(
                    "boto3 não disponível. Instale com: pip install boto3"
                )
                raise ImportError(
                    "boto3 é necessário para detecção via metadados"
                )

            # Criar cliente Glue
            glue_client = boto3.client("glue")

            try:
                # Obter informações da tabela do Glue Catalog
                response = glue_client.get_table(
                    DatabaseName=database_name, Name=table_name
                )

                table_info = response["Table"]

                # Verificar propriedades da tabela para detectar Iceberg
                table_type = table_info.get("TableType", "")
                parameters = table_info.get("Parameters", {})
                storage_descriptor = table_info.get("StorageDescriptor", {})

                # Verificar se é tabela Iceberg
                if self._is_iceberg_table(
                    table_type, parameters, storage_descriptor
                ):
                    self.logger.info(
                        f"Tabela {database_name}.{table_name} detectada como Iceberg via metadados"
                    )
                    return "iceberg"

                # Se não for Iceberg, usar formato padrão
                self.logger.info(
                    f"Tabela {database_name}.{table_name} usando formato padrão"
                )
                return "standard"

            except ClientError as e:
                if e.response["Error"]["Code"] == "EntityNotFoundException":
                    self.logger.error(
                        f"Tabela {database_name}.{table_name} não encontrada no Glue Catalog"
                    )
                    raise
                else:
                    self.logger.error(f"Erro ao acessar metadados: {str(e)}")
                    raise

        except Exception as e:
            self.logger.error(
                f"Erro ao detectar formato via metadados: {str(e)}"
            )
            raise

    def _is_iceberg_table(
        self, table_type: str, parameters: dict, storage_descriptor: dict
    ) -> bool:
        """
        Verifica se uma tabela é Iceberg baseado nos metadados

        Args:
            table_type: Tipo da tabela
            parameters: Parâmetros da tabela
            storage_descriptor: Descritor de armazenamento

        Returns:
            True se for tabela Iceberg
        """
        # Verificar propriedades específicas do Iceberg
        iceberg_indicators = [
            # Verificar se é tabela externa Iceberg
            table_type == "EXTERNAL_TABLE"
            and "iceberg" in parameters.get("table_type", "").lower(),
            # Verificar parâmetros específicos do Iceberg
            "iceberg.table.format-version" in parameters,
            "iceberg.metadata.location" in parameters,
            "iceberg.catalog" in parameters,
            # Verificar se o input format é Iceberg
            storage_descriptor.get("InputFormat", "").startswith(
                "org.apache.iceberg"
            ),
            # Verificar se há propriedades Iceberg
            any("iceberg" in key.lower() for key in parameters.keys()),
            # Verificar se o serde é Iceberg
            "iceberg"
            in storage_descriptor.get("SerdeInfo", {})
            .get("SerializationLibrary", "")
            .lower(),
        ]

        return any(iceberg_indicators)

    def read_table(
        self,
        database_name: str,
        table_name: str,
        columns: list = None,
        where: str = None,
    ) -> DataFrame:
        """
        Lê uma tabela do catálogo com detecção automática de formato

        Args:
            database_name: Nome do banco de dados
            table_name: Nome da tabela
            columns: Lista de colunas para selecionar (opcional)
            where: Condição WHERE para filtrar dados (opcional)

        Returns:
            DataFrame com os dados da tabela
        """
        try:
            # Detecção direta via metadados (já é muito rápida)
            table_format = self.detect_table_format(database_name, table_name)

            if table_format == "iceberg":
                return self.read_iceberg_table_from_catalog(
                    database_name, table_name, columns, where
                )
            else:
                return self.read_table_from_catalog(
                    database_name, table_name, columns, where
                )

        except Exception as e:
            self.logger.error(
                f"Erro ao ler tabela {database_name}.{table_name}: {str(e)}"
            )
            raise

    def read_file(
        self,
        file_path: str,
        format_type: str = "parquet",
        options: Dict[str, Any] = None,
    ) -> DataFrame:
        """
        Lê arquivos de diferentes formatos (Parquet, CSV, Excel)

        Args:
            file_path: Caminho do arquivo
            format_type: Tipo de formato ('parquet', 'csv', 'excel')
            options: Opções específicas do formato

        Returns:
            DataFrame com os dados do arquivo
        """
        try:
            self.logger.info(f"Lendo arquivo {format_type} de: {file_path}")
            if options is None:
                options = {}

            if format_type.lower() == "csv":
                csv_options = {
                    "header": "true",
                    "delimiter": ",",
                    "inferSchema": "true",
                    "encoding": "UTF-8",
                }
                csv_options.update(options)
                df = self._spark_session.read
                for key, value in csv_options.items():
                    df = df.option(key, value)
                df = df.csv(file_path)

            elif format_type.lower() == "excel":
                try:
                    import pandas as pd

                    excel_options = {
                        "sheet_name": 0,
                        "header": 0,
                        "engine": "openpyxl",
                    }
                    excel_options.update(options)
                    pdf = pd.read_excel(
                        file_path,
                        sheet_name=excel_options.get("sheet_name", 0),
                        header=excel_options.get("header", 0),
                        engine=excel_options.get("engine", "openpyxl"),
                    )
                    df = self._spark_session.createDataFrame(pdf)
                except ImportError:
                    raise ImportError(
                        "pandas é necessário para ler arquivos Excel. "
                        "Instale com: pip install pandas openpyxl"
                    )
            else:
                parquet_options = {
                    "compression": "snappy",
                    "mergeSchema": "false",
                    "columnarReaderBatchSize": "4096",
                }
                parquet_options.update(options)
                df = self._spark_session.read
                for key, value in parquet_options.items():
                    df = df.option(key, value)
                df = df.parquet(file_path)

            self.logger.info(f"Arquivo {format_type} lido com sucesso!")
            return df
        except Exception as e:
            self.logger.error(
                f"Erro ao ler {format_type} de {file_path}: {str(e)}"
            )
            raise

    def write_file(
        self,
        df: DataFrame,
        file_path: str,
        format_type: str = "parquet",
        mode: str = "overwrite",
        options: Dict[str, Any] = None,
    ) -> None:
        """
        Escreve DataFrames em diferentes formatos (Parquet, CSV)

        Args:
            df: DataFrame para escrever
            file_path: Caminho do arquivo de destino
            format_type: Tipo de formato ('parquet', 'csv')
            mode: Modo de escrita ('overwrite', 'append', 'error', 'ignore')
            options: Opções específicas do formato
        """
        try:
            self.logger.info(
                f"Escrevendo DataFrame em {format_type}: {file_path}"
            )
            if options is None:
                options = {}

            # Configurar writer com formato e modo
            writer = df.write.format(format_type).mode(mode)

            # Aplicar opções específicas do formato
            if format_type.lower() == "csv":
                csv_options = {
                    "header": "true",
                    "delimiter": ",",
                    "encoding": "UTF-8",
                }
                csv_options.update(options)

                for key, value in csv_options.items():
                    writer = writer.option(key, value)

            elif format_type.lower() == "parquet":
                parquet_options = {
                    "compression": "snappy",
                }
                parquet_options.update(options)

                for key, value in parquet_options.items():
                    writer = writer.option(key, value)

            # Salvar arquivo
            writer.save(file_path)

            self.logger.info(
                f"DataFrame escrito com sucesso em {format_type}!"
            )

        except Exception as e:
            self.logger.error(
                f"Erro ao escrever DataFrame em {format_type}: {str(e)}"
            )
            raise

    def write_table(
        self,
        df: DataFrame,
        database_name: str,
        table_name: str,
        mode: str = "overwrite",
        compression: str = "snappy",
    ) -> None:
        """
        Escreve DataFrame como tabela no Glue Catalog

        Args:
            df: DataFrame para escrever
            database_name: Nome do banco de dados
            table_name: Nome da tabela
            mode: Modo de escrita ('overwrite', 'append', 'error', 'ignore')
            compression: Compressão para Parquet
        """
        try:
            self.logger.info(
                f"Escrevendo DataFrame como tabela: {database_name}.{table_name}"
            )

            # Escrever usando GlueContext
            self._glue_context.write_dynamic_frame.from_options(
                frame=self._glue_context.create_dynamic_frame.from_dataframe(
                    df, self._spark_session
                ),
                connection_type="marketplace.spark",
                connection_options={
                    "path": f"s3://your-bucket/{database_name}/{table_name}/",
                    "partitionKeys": [],
                },
                transformation_ctx=f"{table_name}_ctx",
            )

            self.logger.info(
                f"Tabela {database_name}.{table_name} escrita com sucesso!"
            )

        except Exception as e:
            self.logger.error(
                f"Erro ao escrever tabela {database_name}.{table_name}: {str(e)}"
            )
            raise

    def sql(self, query: str) -> DataFrame:
        """
        Executa uma query SQL usando o SparkSession interno

        Args:
            query: Query SQL para executar

        Returns:
            DataFrame com o resultado da query
        """
        try:
            self.logger.info(f"Executando query SQL: {query[:100]}...")
            return self._spark_session.sql(query)
        except Exception as e:
            self.logger.error(f"Erro ao executar query SQL: {str(e)}")
            raise

    def createDataFrame(
        self, data, schema=None, samplingRatio=None, verifySchema=True
    ):
        """
        Cria um DataFrame usando o SparkSession interno

        Args:
            data: Dados para criar o DataFrame
            schema: Schema do DataFrame (opcional)
            samplingRatio: Taxa de amostragem (opcional)
            verifySchema: Verificar schema (opcional)

        Returns:
            DataFrame criado
        """
        try:
            self.logger.info("Criando DataFrame...")
            return self._spark_session.createDataFrame(
                data, schema, samplingRatio, verifySchema
            )
        except Exception as e:
            self.logger.error(f"Erro ao criar DataFrame: {str(e)}")
            raise

    def get_table_info(
        self, database_name: str, table_name: str
    ) -> Dict[str, Any]:
        """
        Obtém informações sobre uma tabela do Glue Catalog

        Args:
            database_name: Nome do banco de dados
            table_name: Nome da tabela

        Returns:
            Dicionário com informações da tabela
        """
        try:
            # Usar o método read_table que já tem cache de formato
            df = self.read_table(database_name, table_name)

            info = {
                "database": database_name,
                "table": table_name,
                "schema": df.schema,
                "columns": df.columns,
                "count": df.count(),
                "partition_columns": df.rdd.getNumPartitions(),
            }

            return info

        except Exception as e:
            self.logger.error(f"Erro ao obter informações da tabela: {str(e)}")
            raise
