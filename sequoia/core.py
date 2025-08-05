"""
Utilitários para operações Spark no AWS Glue

Autor: Gabriel Vieira
Data: 2025
Versão: 1.0.0
"""

import sys
import warnings
from typing import Any, Dict

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession

from sequoia.logger import Logger

# Suprimir warnings desnecessários globalmente
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)


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
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
            "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
            "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
            "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
            "hive.exec.dynamic.partition": "true",
            "hive.exec.dynamic.partition.mode": "nonstrict",
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

    def read_table(
        self,
        database: str,
        table: str,
        columns: list = None,
        where: str = None,
    ) -> DataFrame:
        """
        Lê uma tabela do catálogo usando Spark DataFrame

        Args:
            database: Nome do banco de dados
            table: Nome da tabela
            columns: Lista de colunas para selecionar (opcional)
            where: Condição WHERE para filtrar dados (opcional)

        Returns:
            DataFrame com os dados da tabela
        """
        try:
            # Construir query SQL
            if columns:
                columns_str = ", ".join(columns)
                query = f"SELECT {columns_str} FROM {database}.{table}"
            else:
                query = f"SELECT * FROM {database}.{table}"

            # Adicionar condição WHERE se fornecida
            if where:
                query += f" WHERE {where}"

            df = self._spark_session.sql(query)
            return df

        except Exception as e:
            self.logger.error(
                f"Erro ao ler tabela {database}.{table}: {str(e)}"
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
                        "sheet": 0,
                        "header": 0,
                        "engine": "openpyxl",
                    }
                    excel_options.update(options)
                    pdf = pd.read_excel(
                        file_path,
                        sheet_name=excel_options.get("sheet", 0),
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

        except Exception as e:
            self.logger.error(
                f"Erro ao escrever DataFrame em {format_type}: {str(e)}"
            )
            raise

    def write_table(
        self,
        df: DataFrame,
        database: str,
        table: str,
        mode: str = "overwrite",
        compression: str = "snappy",
    ) -> None:
        """
        Escreve DataFrame como tabela no Glue Catalog usando Spark

        Args:
            df: DataFrame para escrever
            database: Nome do banco de dados
            table: Nome da tabela
            mode: Modo de escrita ('overwrite', 'append', 'error', 'ignore')
            compression: Compressão para Parquet
        """
        try:
            # Escrever usando Spark DataFrame
            df.write.format("parquet").mode(mode).option(
                "compression", compression
            ).saveAsTable(f"{database}.{table}")

        except Exception as e:
            self.logger.error(
                f"Erro ao escrever tabela {database}.{table}: {str(e)}"
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
            return self._spark_session.createDataFrame(
                data, schema, samplingRatio, verifySchema
            )
        except Exception as e:
            self.logger.error(f"Erro ao criar DataFrame: {str(e)}")
            raise

    def get_partitions(self, database: str, table: str) -> list:
        """
        Obtém todas as partições de uma tabela

        Args:
            database: Nome do banco de dados
            table: Nome da tabela

        Returns:
            Lista com todas as partições da tabela no formato coluna=valor
        """
        try:
            import boto3
            from botocore.exceptions import ClientError

            # Criar cliente Glue
            glue_client = boto3.client("glue")

            # Obter informações da tabela para pegar as colunas de partição
            table_response = glue_client.get_table(
                DatabaseName=database, TableName=table
            )
            partition_keys = table_response["Table"].get("PartitionKeys", [])
            partition_column_names = [key["Name"] for key in partition_keys]

            # Obter partições da tabela usando paginação
            partition_strings = []
            paginator = glue_client.get_paginator("get_partitions")

            for page in paginator.paginate(
                DatabaseName=database, TableName=table
            ):
                partitions = page.get("Partitions", [])

                for partition in partitions:
                    values = partition.get("Values", [])
                    if values and len(values) == len(partition_column_names):
                        # Formatar como string: coluna1=valor1,coluna2=valor2
                        partition_parts = []
                        for i, column_name in enumerate(
                            partition_column_names
                        ):
                            partition_parts.append(
                                f"{column_name}={values[i]}"
                            )
                        partition_str = ",".join(partition_parts)
                        partition_strings.append(partition_str)

            return partition_strings

        except ImportError:
            self.logger.error(
                "boto3 não disponível. Instale com: pip install boto3"
            )
            raise ImportError(
                "boto3 é necessário para obter partições da tabela"
            )

        except ClientError as e:
            self.logger.error(f"Erro ao obter partições da tabela: {str(e)}")
            raise

        except Exception as e:
            self.logger.error(f"Erro inesperado ao obter partições: {str(e)}")
            raise

    def get_last_partition(self, database: str, table: str) -> str:
        """
        Obtém a última partição de uma tabela

        Args:
            database: Nome do banco de dados
            table: Nome da tabela

        Returns:
            String com a última partição da tabela no formato coluna=valor
        """
        try:
            # Obter todas as partições
            partitions = self.get_partitions(database, table)

            if not partitions:
                self.logger.warning(
                    f"Nenhuma partição encontrada para {database}.{table}"
                )
                return ""

            # Ordenar partições pelo último valor (assumindo que é a data mais recente)
            # Exemplo: "ano=2025,mes=08,dia=05" -> ordenar por "05"
            def get_last_value(partition):
                return partition.split(",")[-1].split("=")[1]

            sorted_partitions = sorted(partitions, key=get_last_value)

            last_partition = sorted_partitions[-1]
            return last_partition

        except Exception as e:
            self.logger.error(f"Erro ao obter última partição: {str(e)}")
            raise

    def _get_aws_account(self) -> str:
        """
        Obtém o ID da conta AWS atual

        Returns:
            ID da conta AWS (12 dígitos)
        """
        try:
            import boto3
            from botocore.exceptions import ClientError

            # Criar cliente STS para obter informações da conta
            sts_client = boto3.client("sts")

            # Obter informações da conta atual
            response = sts_client.get_caller_identity()
            account_id = response["Account"]

            return account_id

        except ImportError:
            self.logger.error(
                "boto3 não disponível. Instale com: pip install boto3"
            )
            raise ImportError(
                "boto3 é necessário para obter informações da conta AWS"
            )

        except ClientError as e:
            self.logger.error(
                f"Erro ao obter informações da conta AWS: {str(e)}"
            )
            raise

        except Exception as e:
            self.logger.error(f"Erro inesperado ao obter conta AWS: {str(e)}")
            raise
