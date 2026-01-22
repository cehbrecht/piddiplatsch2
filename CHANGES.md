# Changelog

All notable changes to this project are documented here.

## [Unreleased]
- No changes yet.

## [2.1.0] - 2026-01-22
### Added
- Run retry via `RetryRunner` class with `run_file` and `run_batch`.
- Common result module at `piddiplatsch.result` consolidating dataclasses.
- Common helpers at `piddiplatsch.helpers`: `DailyJsonlWriter`, `read_jsonl`, `find_jsonl`, `utc_now`.
- JSONL handle backend now uses shared helpers (daily rotation, UTC timestamps).
- Retry CLI supports files/dirs/globs, skipped items, `--dry-run`, `--delete-after`, and `-v`.
- New `--force` option to continue despite transient external failures (records skipped items).

### Changed
- Unified persistence API: `RecorderBase.record()` handles infos and writes to JSONL files.
- Retry logic migrated to class-based `persist/retry`.
- Standardized timestamps to UTC via `utc_now()`.
- Simplified logging and persistence flows.


## [2.0.0] - 2026-01-13
- Initial project setup.
- Add bump-my-version configuration and Makefile targets for patch/minor/major bumps.
- Update README with concise tagline and brief versioning usage.
- Establish Kafka consumer, CLI entry point, and basic tooling.

