# Changelog

All notable changes to this project are documented here.

## [Unreleased]
### Added
- Transient external failure policy with bounded retries and exponential backoff for STAC fetch.
- `--force` option to continue consumption despite transient external failures (records skipped items).
- Skipped message persistence to `outputs/skipped/` with reason metadata (`__infos__`).
- STAC preflight health check (configurable via `consumer.preflight_stac`).
- External failures counter in stats and SQLite reporter.
 - Abstract `RecorderBase` for persistence recorders to unify common behavior.

### Changed
- Missing payload/item and invalid JSON Patch now treated as errors (count toward `max_errors`).
- Retry command supports processing skipped JSONL files alongside failures.
 - Replaced legacy persistence methods (`record_item`, `record_skipped_item`, `record_failed_item`) with a unified `record(key, data, reason=None, retries=None)` API across `DumpRecorder`, `SkipRecorder`, and `FailureRecovery`.

## [2.0.0] - 2026-01-13
- Initial project setup.
- Add bump-my-version configuration and Makefile targets for patch/minor/major bumps.
- Update README with concise tagline and brief versioning usage.
- Establish Kafka consumer, CLI entry point, and basic tooling.

