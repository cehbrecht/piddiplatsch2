import click
import logging
from piddiplatsch.consumer import create_consumer, process_message

# Kafka configuration
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "CMIP7"


def start_consumer():
    logging.basicConfig(level=logging.INFO)
    consumer = create_consumer(KAFKA_TOPIC, KAFKA_SERVER)

    click.echo("Starting Kafka consumer...")
    for message in consumer:
        process_message(message.value)


CONTEXT_OBJ = dict()
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"], obj=CONTEXT_OBJ)


@click.group()
def cli():
    pass


@cli.command()
def consume():
    "Start the Kafka consumer"
    start_consumer()


if __name__ == "__main__":
    cli()
