import logging
from logging.handlers import RotatingFileHandler
import sys
from pathlib import Path
from typing import Dict


class LoggerFactory:
    _instances: Dict[str, logging.Logger] = {}

    @classmethod
    def get_logger(cls, name: str, log_file: str = "logs/app.log", level: int = logging.DEBUG) -> logging.Logger:
        if name in cls._instances:
            return cls._instances[name]

        logger = logging.getLogger(name)
        logger.setLevel(level)

        # ⚡ Évite les doublons de handlers
        if not logger.handlers:
            # === Handler console ===
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setLevel(logging.INFO)  # exemple : console moins verbeuse
            stream_format = logging.Formatter(
                "[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            stream_handler.setFormatter(stream_format)

            # === Handler rotating file ===
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=5 * 1024 * 1024,  # 5 MB
                backupCount=10,
                encoding="utf-8"
            )
            file_handler.setLevel(logging.DEBUG)  # fichier plus verbeux
            file_format = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(file_format)

            # Ajout
            logger.addHandler(stream_handler)
            logger.addHandler(file_handler)

        cls._instances[name] = logger
        return logger
