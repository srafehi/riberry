# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [0.4.1] - 2019-01-23
### Changed
- Removed all constraints applied when updating existing input fields.

### Fixed
- Allow blank dropdown values.

## [0.4.0] - 2018-12-02
### Added
- Priority queue now blocks by default when the value returned is
  empty. This can be disabled by setting the input parameter
  `DynamicPriorityParameter.blocking` to `False`.
- Capacity distribution weight parameters can now be scheduled
- Riberry will track `READY` and `ACTIVE` execution task IDs in Redis.
  If these executions are still marked as `READY` or `ACTIVE` but their
  task IDs don't exist in Redis anymore, the execution is cancelled due
  to the assumption that Redis itself was flushed and all progress for
  that execution was lost.

### Fixed
- Application instance schedules now accept `0` as a valid value

## [0.3.2] - 2018-10-19
### Added
- Added capacity distribution strategies: `binpack` (default) and
  `spread`

## [0.3.1] - 2018-10-17
### Added
- ApplicationSchedule.value is now nullable

## [0.3.0] - 2018-10-11
### Added
- Added cross-application capacity allocation, enables distributed
  capacity load balancing

## [0.2.4] - 2018-08-28
### Fixed
- Configuration importing doesn't break during `import_groups` step
  when there's an application not yet loaded in the database

## [0.2.3] - 2018-08-28
### Added
- Prevent empty-string streams from being entered on client-side,
  completely ignore server-side empty-string streams

## [0.2.2] - 2018-08-27
### Added
- Added first round of integration tests, tests Celery Workflow
  integration

## [0.2.1] - 2018-08-23
### Fixed
- Fixed issue which falsely marked a form's input enumerations as
  changed

## [0.2.0] - 2018-08-22
### Added
- Added `wf.artifact_from_traceback`, simplifies the creation of error
  artifacts
- Added `wf.send_email`, can now send custom emails from workflows

### Fixed
- Fixed issue which caused events to stop processing when defining
  empty-string streams and artifacts with Oracle DB (#3)

## [0.1.0] - 2018-08-13
### Added
- Added `wf.create_job`, allows for the creation of child jobs from a
  current execution
- Added ability to add and remove users from groups via cli
  (`user-groups add/remove`)