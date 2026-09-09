import sys

from loguru import logger


def configure_logging(
    *,
    level: str,
) -> None:
    logger.remove()

    logger.add(
        sys.stderr,
        level=level.upper(),
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> "
            "| <level>{level: <8}</level> "
            "| <cyan>{name}</cyan>:<cyan>{function}</cyan> "
            "| <level>{message}</level>"
        ),
        enqueue=True,
    )
