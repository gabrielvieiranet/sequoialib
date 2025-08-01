"""
Logger utilitário para jobs Glue
"""

import logging


class Logger:
    """
    Classe utilitária para logging em jobs Glue
    """

    def __init__(self, name: str = "GlueJob"):
        """
        Inicializa o logger

        Args:
            name: Nome do logger
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        # Configurar handler se não existir
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def info(self, message: str):
        """
        Log de informação

        Args:
            message: Mensagem para log
        """
        self.logger.info(message)

    def error(self, message: str):
        """
        Log de erro

        Args:
            message: Mensagem para log
        """
        self.logger.error(message)

    def warning(self, message: str):
        """
        Log de aviso

        Args:
            message: Mensagem para log
        """
        self.logger.warning(message)

    def debug(self, message: str):
        """
        Log de debug

        Args:
            message: Mensagem para log
        """
        self.logger.debug(message)
