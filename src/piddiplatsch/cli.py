import click
import logging
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from piddiplatsch.consumer import Consumer, process_message

# Default Kafka server configuration
DEFAULT_KAFKA_SERVER = "localhost:39092"


def start_consumer(topic, kafka_server):
    logging.info(f"Starting Kafka consumer for topic: {topic}...")
    consumer = Consumer(topic, kafka_server)
    for message in consumer.consume():
        process_message(message)


CONTEXT_OBJ = dict()
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"], obj=CONTEXT_OBJ)

@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
@click.pass_context
def cli(ctx):
    """CLI to interact with Kafka and Handle Service."""
    ctx.ensure_object(dict)


@cli.command()
@click.pass_context
@click.option(
    "--topic", "-t", default="CMIP7", help="Kafka topic to consume messages from (default: CMIP7)"
)
@click.option(
    "--kafka-server", "-s", default=DEFAULT_KAFKA_SERVER, help="Kafka server (default: localhost:39092)"
)
def consume(ctx, topic, kafka_server):
    """Start the Kafka consumer."""
    start_consumer(topic, kafka_server)


@cli.command()
@click.pass_context
@click.option(
    "--topic", "-t", default="CMIP7", help="Kafka topic to create (default: CMIP7)"
)
@click.option(
    "--kafka-server", "-s", default=DEFAULT_KAFKA_SERVER, help="Kafka server (default: localhost:39092)"
)
def init(ctx, topic, kafka_server):
    """Create the Kafka topic if it doesn't exist."""
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_server)
    topic_list = [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        click.echo(f"Created Kafka topic: {topic}")
    except Exception as e:
        click.echo(f"Error creating topic: {e}")


@cli.command()
@click.pass_context
@click.option(
    "--message", "-m", required=True, help="Message to send to Kafka topic"
)
@click.option(
    "--topic", "-t", default="CMIP7", help="Kafka topic to send messages to (default: CMIP7)"
)
@click.option(
    "--kafka-server", "-s", default=DEFAULT_KAFKA_SERVER, help="Kafka server (default: localhost:39092)"
)
def send(ctx, message, topic, kafka_server):
    """Send a message to the Kafka topic."""
    producer = KafkaProducer(bootstrap_servers=kafka_server)
    producer.send(topic, message.encode("utf-8"))
    producer.flush()
    click.echo(f"Sent message to {topic}: {message}")


if __name__ == "__main__":
    cli()
