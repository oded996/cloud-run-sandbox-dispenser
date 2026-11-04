# Contributing to Cloud Run Sandbox Dispenser (`sbx`)

We welcome contributions, bug reports, documentation improvements, and feature requests!

## Contributor License Agreement (CLA)

Contributions to this project must be accompanied by a [Contributor License Agreement](https://cla.developers.google.com/about) (CLA). You (or your employer) retain the copyright to your contribution; this simply gives us permission to use and redistribute your contributions as part of the project.

If you or your current employer have already signed the Google CLA (even if it was for a different project), you probably do not need to sign it again.

Visit <https://cla.developers.google.com/> to see your current agreements or to sign a new one.

## Development Workflow

1. **Prerequisites**: Go 1.23+ (uses only the Go standard library; zero third-party runtime dependencies).
2. **Build the binary**:
   ```shell
   make build
   ```
3. **Install locally (`sbx` in `$GOBIN` or `$GOPATH/bin`)**:
   ```shell
   make install
   ```
4. **Format and run the test suite**:
   ```shell
   gofmt -s -w .
   make test
   ```
   Please run `gofmt -s -w .` and `make test` locally before opening a pull request.

## Submitting Changes

* Open an issue first for major architectural changes to discuss the design.
* Keep pull requests focused and include unit or integration test coverage in `dispenser_test.go`.
* Update `USER_GUIDE.md`, `ARCHITECTURE.md`, or `README.md` when adding or changing CLI flags or endpoints.

## Community Guidelines & Code of Conduct

This project follows [Google's Open Source Community Guidelines](https://opensource.google/conduct/). Please be respectful and constructive in all interactions.
