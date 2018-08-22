# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).


## [0.2.0] - 2018-08-22
### Added
- Added `wf.artifact_from_traceback`, simplifies the creation of error artifacts
- Added `wf.send_email`, can now send custom emails from workflows

### Fixed
- Events stop processing when defining empty-string streams and artifacts when using Oracle DB (#3)

## [0.1.0] - 2018-08-13
### Added
- Added `wf.create_job`, allows for the creation of child jobs from a current execution
- Added ability to add and remove users from groups via cli (`user-groups add/remove`)