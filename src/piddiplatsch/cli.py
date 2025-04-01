import click
import logging
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
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


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
@click.pass_context
def cli(ctx):
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)


@cli.command()
@click.pass_context
def init(ctx):
    "Create the Kafka CMIP7 topic if it doesn't exist"
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)
    topic_list = [NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)]
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        click.echo("Created Kafka topic: CMIP7")
    except Exception as e:
        click.echo(f"Error creating topic: {e}")


@cli.command()
@click.pass_context
def consume(ctx):
    "Start the Kafka consumer"
    start_consumer()


@cli.command()
@click.pass_context
@click.option(
    "--message", "-m", required=True, help="Message to send to Kafka topic CMIP7"
)
def send(ctx, message):
    "Send a message to the Kafka CMIP7 topic"
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)
    producer.send(KAFKA_TOPIC, message.encode("utf-8"))
    producer.flush()
    click.echo(f"Sent message to CMIP7: {message}")


if __name__ == "__main__":
    cli()
