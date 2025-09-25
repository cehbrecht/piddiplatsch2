import json
from pathlib import Path

import click

from piddiplatsch.config import config
from piddiplatsch.consumer import start_consumer
from piddiplatsch.persist.recovery import FailureRecovery
from piddiplatsch.testing import kafka_client

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
@click.option(
    "-c",
    "--config",
    "config_file",
    type=click.Path(),
    help="Path to custom config TOML file.",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option("-v", "--verbose", is_flag=True, help="Show progress bar.")
@click.option(
    "-l",
    "--log",
    type=click.Path(dir_okay=False, writable=True, resolve_path=True),
    default="pid.log",
    show_default=True,
    help="Log file path.",
)
@click.pass_context
def cli(ctx, config_file, debug, verbose, log):
    """CLI to interact with Kafka and Handle Service."""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    config.load_user_config(config_file)
    config.configure_logging(debug=debug, log=log)


# init command


@cli.command()
def init():
    """Creates the kafka topic."""
    topic = config.get("consumer", "topic")
    kafka_cfg = config.get("kafka")
    kafka_client.ensure_topic_exists(topic, kafka_cfg)


# consume command


@cli.command()
@click.option("--dump", is_flag=True, help="Dump all consumed messages to JSONL files.")
@click.pass_context
def consume(ctx, dump):
    """Start the Kafka consumer."""
    topic = config.get("consumer", "topic")
    kafka_cfg = config.get("kafka")
    processor = config.get("plugin", "processor")
    start_consumer(
        topic, kafka_cfg, processor, dump_messages=dump, verbose=ctx.obj["verbose"]
    )


# retry command


@cli.command("retry")
@click.argument(
    "filename", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.option(
    "--delete-after",
    is_flag=True,
    help="Delete the file if all messages are retried successfully.",
)
def retry(filename: Path, delete_after: bool):
    """Retry failed items from a failure .jsonl file."""
    retry_topic = config.get("consumer", "retry_topic")
    kafka_cfg = config.get("kafka", {})
    success, failed = FailureRecovery.retry(
        retry_topic=retry_topic,
        kafka_cfg=kafka_cfg,
        jsonl_path=filename,
        delete_after=delete_after,
    )
    click.echo(f"Retried {success} messages, {failed} failed.")


# send command


@cli.command("send")
@click.argument(
    "filename", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.option(
    "--verbose", is_flag=True, help="Show message key and value before sending."
)
@click.pass_context
def send(ctx, filename: Path, verbose: bool):
    """Send a message to the Kafka queue."""
    try:
        key, value = kafka_client.build_message_from_path(filename)
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
        topic = config.get("consumer", "topic")
        kafka_cfg = config.get("kafka")
        kafka_client.send_message(topic, kafka_cfg, key, value, on_delivery=report)
    except Exception as e:
        click.echo(f"❌ {e}", err=True)
        ctx.exit(1)


if __name__ == "__main__":
    cli()
