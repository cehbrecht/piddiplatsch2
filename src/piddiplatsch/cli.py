from pathlib import Path

import click

from piddiplatsch.config import config
from piddiplatsch.consumer import start_consumer
from piddiplatsch.persist.recovery import FailureRecovery

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


# consume command


@cli.command()
@click.option("--dump", is_flag=True, help="Dump all consumed messages to JSONL files.")
@click.option(
    "--dry-run",
    is_flag=True,
    help="Do not publish to handle server; write handles to JSONL only.",
)
@click.pass_context
def consume(ctx, dump, dry_run):
    """Start the Kafka consumer."""
    topic = config.get("consumer", "topic")
    kafka_cfg = config.get("kafka")
    processor = config.get("consumer", "processor")
    start_consumer(
        topic,
        kafka_cfg,
        processor,
        dump_messages=dump,
        verbose=ctx.obj["verbose"],
        dry_run=dry_run,
    )


# retry command


@cli.command("retry")
@click.argument(
    "path",
    type=click.Path(exists=True, path_type=Path),
    nargs=-1,
    required=True,
)
@click.option(
    "--delete-after",
    is_flag=True,
    help="Delete files after successful retry.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Write handles to JSONL without contacting Handle Service.",
)
@click.pass_context
def retry(ctx, path: tuple[Path, ...], delete_after: bool, dry_run: bool):
    """Retry failed items from failure .jsonl file(s) or directory.

    Accepts multiple arguments:
    - Individual files: retry file1.jsonl file2.jsonl
    - Directories: retry outputs/failures/r0/
    - Glob patterns: retry outputs/failures/r0/*.jsonl
    """
    processor = config.get("consumer", "processor")
    verbose = ctx.obj.get("verbose", False)

    # Define progress callback for verbose mode
    def show_progress(file, idx, total, result):
        if verbose:
            click.echo(f"[{idx}/{total}] {file.name}: ", nl=False)
            if result.total > 0:
                click.echo(
                    f"{result.succeeded}/{result.total} succeeded"
                    + (f", {result.failed} failed" if result.failed > 0 else "")
                )
            else:
                click.echo("(empty)")

    result = FailureRecovery.retry_batch(
        path,
        processor=processor,
        delete_after=delete_after,
        dry_run=dry_run,
        verbose=verbose,
        progress_callback=show_progress if verbose else None,
    )

    if result.total == 0:
        click.echo("No retry files found.")
        return

    # Show overall summary
    click.echo(f"\nTotal: {result.succeeded}/{result.total} succeeded")
    if result.failed > 0:
        click.echo(
            f"  ⚠️  {result.failed} items failed again ({result.success_rate:.1f}% success rate)"
        )
        if result.failure_files:
            click.echo("  New failures saved to:")
            for failure_file in sorted(result.failure_files):
                rel_path = failure_file.relative_to(FailureRecovery.FAILURE_DIR)
                click.echo(f"    - {rel_path}")
    else:
        click.echo("  ✓ All items processed successfully!")


if __name__ == "__main__":
    cli()
