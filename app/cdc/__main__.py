"""Entrypoint: `python -m app.cdc`."""

from ..config import settings
from ..logging_config import setup_logging
from ..tracing import setup_tracing
from . import metrics
from .consumer import UserCdcConsumer, build_kafka_consumer

setup_logging(settings.log_level)
setup_tracing("user-service-cdc")


def main() -> None:
    metrics.start_metrics_server(settings.cdc_metrics_port)
    consumer = UserCdcConsumer(build_kafka_consumer())
    consumer.run_forever()


if __name__ == "__main__":
    main()
