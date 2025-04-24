import click
import os
import logging
import json
import uuid
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from piddiplatsch.consumer import Consumer, process_message

# Default Kafka server configuration
DEFAULT_KAFKA_SERVER = "localhost:39092"


def start_consumer(topic, kafka_server):
    logging.info(f"Starting Kafka consumer for topic: {topic}...")
    consumer = Consumer(topic, kafka_server)
    for key, value in consumer.consume():
        process_message(key, value)


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
    "--topic",
    "-t",
    default="CMIP7",
    help="Kafka topic to consume messages from (default: CMIP7)",
)
@click.option(
    "--kafka-server",
    "-s",
    default=DEFAULT_KAFKA_SERVER,
    help="Kafka server (default: localhost:39092)",
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
    "--kafka-server",
    "-s",
    default=DEFAULT_KAFKA_SERVER,
    help="Kafka server (default: localhost:39092)",
)
def init(ctx, topic, kafka_server):
    """Create the Kafka topic if it doesn't exist."""
    admin_client = AdminClient({"bootstrap.servers": kafka_server})
    new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
    fs = admin_client.create_topics([new_topic])

    try:
        fs[topic].result()
        click.echo(f"Created Kafka topic: {topic}")
    except Exception as e:
        click.echo(f"Error creating topic: {e}")


@cli.command()
@click.pass_context
@click.option(
    "--message",
    "-m",
    required=False,
    help="A message (json string) to send to Kafka topic.",
)
@click.option(
    "--path", "-p", required=False, help="A JSON file to send to Kafka topic."
)
@click.option(
    "--topic",
    "-t",
    default="CMIP7",
    help="Kafka topic to send messages to (default: CMIP7)",
)
@click.option(
    "--kafka-server",
    "-s",
    default=DEFAULT_KAFKA_SERVER,
    help="Kafka server (default: localhost:39092)",
)
def send(ctx, message, path, topic, kafka_server):
    """Send a message to the Kafka topic."""
    producer = Producer({"bootstrap.servers": kafka_server})

    if path is not None:
        with open(path, "r") as f:
            data = json.load(f)
            key = os.path.splitext(os.path.basename(path))[0]
            value = json.dumps(data)
    elif message is not None:
        key = str(uuid.uuid5(uuid.NAMESPACE_DNS, message))
        value = message
    else:
        click.echo(f"Please provide a message.")

    def delivery_report(err, msg):
        if err is not None:
            click.echo(f"Delivery failed: {err}")
        else:
            click.echo(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    # Send the message (assuming it's a stringified JSON or simple string)
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=value.encode("utf-8"),
        callback=delivery_report,
    )
    producer.flush()


if __name__ == "__main__":
    cli()
