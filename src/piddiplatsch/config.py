import logging


def setup_logging(debug=False, logfile=None):
    log_level = logging.DEBUG if debug else logging.INFO
    log_format = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    handlers = []

    try:
        from colorlog import ColoredFormatter

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(
            ColoredFormatter(
                "%(log_color)s%(asctime)s - %(levelname)-8s - %(name)s - %(message)s",
                log_colors={
                    "DEBUG": "cyan",
                    "INFO": "green",
                    "WARNING": "yellow",
                    "ERROR": "red",
                    "CRITICAL": "bold_red",
                },
            )
        )
    except ImportError:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(log_format))

    handlers.append(console_handler)

    if logfile:
        file_handler = logging.FileHandler(logfile)
        file_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(file_handler)

    logging.basicConfig(level=log_level, handlers=handlers)
