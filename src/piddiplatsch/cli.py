import click
import os
import logging
import json
from piddiplatsch.consumer import start_consumer
from piddiplatsch.config import config
from piddiplatsch import client

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


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

## init command

@cli.command()
@click.option(
    "-t",
    "--topic",
    default=config.get("kafka", "topic"),
    help="Kafka topic to consume from.",
)
def init(topic):
    """Creates the kafka topic."""
    kafka_cfg = config.get("kafka")
    client.ensure_topic_exists(topic, kafka_cfg)


## consume command

@cli.command()
@click.option(
    "-t",
    "--topic",
    default=config.get("consumer", "topic"),
    help="Kafka topic to consume from.",
)
def consume(topic):
    """Start the Kafka consumer."""
    kafka_cfg = config.get("kafka")
    start_consumer(topic, kafka_cfg)


## send command

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
    "--verbose", is_flag=True, help="Show message key and value before sending."
)
@click.pass_context
def send(ctx, message, path, topic, verbose):
    """Send a message to the Kafka topic."""
    if message and path:
        click.echo("❌ Provide only one of --message or --path.", err=True)
        ctx.exit(1)

    if not message and not path:
        click.echo("❌ Please provide a message or a path to a JSON file.", err=True)
        ctx.exit(1)

    try:
        if path:
            key, value = client.build_message_from_path(path)
        else:
            key, value = client.build_message_from_json_string(message)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        click.echo(f"❌ {e}", err=True)
        ctx.exit(1)

    if verbose:
        click.echo(f"🔑 Key: {key}")
        click.echo(f"📦 Value: {value}")

    def report(err, msg):
        if err:
            click.echo(f"❌ Delivery failed: {err}", err=True)
        else:
            click.echo(f"📤 Message delivered to {msg.topic()} [{msg.partition()}]")

    try:
        kafka_cfg = config.get("kafka")
        client.send_message(topic, kafka_cfg, key, value, on_delivery=report)
    except Exception as e:
        click.echo(f"❌ {e}", err=True)
        ctx.exit(1)


if __name__ == "__main__":
    cli()
