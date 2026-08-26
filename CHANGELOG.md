# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- A resumed agent can read its own request metadata again. Previously,
  `command.header.metadata` after a suspend (`ask_user`, or a `call_agent`
  hop) held only the metadata of the message that woke the agent up, and
  everything it had originally been dispatched with was gone. It is now the
  original dispatch metadata with the waking message's merged on top (the
  waking message wins on key collisions). Handlers that treated the resumed
  header's metadata as "only what this hop just sent" will now see additional
  keys. Per-hop trace fields (`trace_parent_span_id`,
  `framework_parent_span_id`, `langfuse_parent_observation_id`) still
  describe the current hop and are not restored from the record. Executions
  recorded before this change are unaffected — they degrade to the previous
  behaviour rather than erroring.

### Added
- Standardized Open Source governance files (CONTRIBUTING, CODE_OF_CONDUCT, SECURITY).
- GitHub Issue and Pull Request templates.
- Dependabot configuration for dependency monitoring.

## [0.2.0] - 2026-05-13
### Initial release
