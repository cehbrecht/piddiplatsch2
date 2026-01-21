# Changelog

All notable changes to this project are documented here.

## [Unreleased]
### Added
- Unified persistence via `RecorderBase` with instance `record()`.
- JSONL helpers: daily rotation and read/find utilities.
- Retry CLI: files/dirs/globs; supports skipped; `--dry-run`, `--delete-after`, `-v`.
 - New `--force` option to continue despite transient external failures (records skipped items).

### Changed
- Invalid payload/item and JSON Patch now count toward `max_errors`.
- Failure recording class renamed to `FailureRecorder`; retry logic moved to `persist/retry`.
- Logging and persistence flows simplified; removed legacy record methods.

## [2.0.0] - 2026-01-13
- Initial project setup.
- Add bump-my-version configuration and Makefile targets for patch/minor/major bumps.
- Update README with concise tagline and brief versioning usage.
- Establish Kafka consumer, CLI entry point, and basic tooling.

