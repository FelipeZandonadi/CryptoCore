from __future__ import annotations
import logging
from logging import Logger
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


DEFAULT_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


def get_logger(
    name: str = __name__,
    level: int = logging.INFO,
    log_file: Optional[str] = 'logs/app.log',
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    fmt: str = DEFAULT_FORMAT,
    force: bool = False,
) -> Logger:
    """
    Retorna um logger configurado com Console + opcional RotatingFileHandler.

    Parâmetros:
    - name: nome do logger.
    - level: nível de logging (logging.DEBUG, INFO, ...).
    - log_file: caminho do arquivo de log (se None, só console).
    - max_bytes: tamanho máximo do arquivo antes de rotacionar.
    - backup_count: quantos arquivos de backup manter.
    - fmt: formato da mensagem de log.
    - force: se True, reconfigura mesmo que handlers já existam.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers and not force:
        logger.setLevel(level)
        return logger

    if force and logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    formatter = logging.Formatter(fmt)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File handler (rotating)
    if log_file:
        log_path = Path(log_file)
        if log_path.parent:
            log_path.parent.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(
            filename=str(log_path),
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8',
        )
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    logger.propagate = False
    return logger
