import click
import os
import logging
import json
import uuid
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from piddiplatsch.consumer import Consumer, process_message
from piddiplatsch.config import config

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


def get_producer(kafka_server):
    return Producer({"bootstrap.servers": kafka_server})


def get_admin_client(kafka_server):
    return AdminClient({"bootstrap.servers": kafka_server})


def ensure_topic_exists(kafka_server, topic):
    admin_client = get_admin_client(kafka_server)
    metadata = admin_client.list_topics(timeout=5)
    if topic not in metadata.topics:
        click.echo(f"ℹ️ Topic '{topic}' does not exist. Creating it...")
        new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        try:
            fs[topic].result()
            click.echo(f"✅ Created Kafka topic: {topic}")
        except Exception as e:
            click.echo(f"⚠️  Failed to create topic '{topic}': {e}", err=True)


def produce_message(kafka_server, topic, key, value):
    ensure_topic_exists(kafka_server, topic)
    producer = get_producer(kafka_server)

    def delivery_report(err, msg):
        if err:
            click.echo(f"❌ Delivery failed: {err}", err=True)
        else:
            click.echo(f"📤 Message delivered to {msg.topic()} [{msg.partition()}]")

    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=value.encode("utf-8"),
        callback=delivery_report,
    )
    producer.flush()


def start_kafka_consumer(topic, kafka_server):
    ensure_topic_exists(kafka_server, topic)
    logging.info(f"Starting Kafka consumer for topic: {topic}")
    consumer = Consumer(topic, kafka_server)
    for key, value in consumer.consume():
        process_message(key, value)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
@click.option(
    "--config",
    "config_file",
    type=click.Path(),
    help="Path to custom config TOML file.",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option(
    "--logfile", type=click.Path(), help="Write logs to file instead of console."
)
@click.pass_context
def cli(ctx, config_file, debug, logfile):
    """CLI to interact with Kafka and Handle Service."""
    ctx.ensure_object(dict)
    config.load_user_config(config_file)
    config.configure_logging(debug=debug, logfile=logfile)


@cli.command()
@click.option(
    "-t",
    "--topic",
    default=config.get("kafka", "topic"),
    help="Kafka topic to consume from.",
)
@click.option(
    "-s",
    "--kafka-server",
    default=config.get("kafka", "server"),
    help="Kafka server URL.",
)
def consume(topic, kafka_server):
    """Start the Kafka consumer."""
    start_kafka_consumer(topic, kafka_server)


@cli.command()
@click.option("-m", "--message", help="Message (JSON string) to send.")
@click.option("-p", "--path", help="Path to JSON file to send.")
@click.option(
    "-t",
    "--topic",
    default=config.get("kafka", "topic"),
    help="Kafka topic to send to.",
)
@click.option(
    "-s",
    "--kafka-server",
    default=config.get("kafka", "server"),
    help="Kafka server URL.",
)
@click.option(
    "--verbose", is_flag=True, help="Show message key and value before sending."
)
@click.pass_context
def send(ctx, message, path, topic, kafka_server, verbose):
    """Send a message to the Kafka topic."""
    if message and path:
        click.echo("❌ Provide only one of --message or --path.", err=True)
        ctx.exit(1)

    if not message and not path:
        click.echo("❌ Please provide a message or a path to a JSON file.", err=True)
        ctx.exit(1)

    if path:
        try:
            with open(path, "r") as f:
                data = json.load(f)
            key = os.path.splitext(os.path.basename(path))[0]
            value = json.dumps(data)
        except FileNotFoundError:
            click.echo(f"❌ File not found: {path}", err=True)
            ctx.exit(1)
        except json.JSONDecodeError as e:
            click.echo(f"❌ Invalid JSON in file: {e}", err=True)
            ctx.exit(1)
    else:
        try:
            data = json.loads(message)
            value = json.dumps(data)
        except json.JSONDecodeError as e:
            click.echo(f"❌ Invalid JSON in --message: {e}", err=True)
            ctx.exit(1)
        key = str(uuid.uuid5(uuid.NAMESPACE_DNS, message))

    if verbose:
        click.echo(f"🔑 Key: {key}")
        click.echo(f"📦 Value: {value}")

    produce_message(kafka_server, topic, key, value)


if __name__ == "__main__":
    cli()
