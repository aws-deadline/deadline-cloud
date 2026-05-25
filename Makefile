PROFILE ?= debug
CARGO_FLAGS := $(if $(filter release,$(PROFILE)),--release,)
TARGET_DIR := target/$(PROFILE)
BIN_NAME := deadline

.PHONY: all build test test-rust test-ui lint fmt check clean setup-tools setup-test

# Install required and recommended Cargo tools
setup-tools:
	cargo install cargo-insta cargo-deny cargo-outdated cargo-bloat

# Default: build
all: build

# Build Rust workspace (all crates)
build:
	cargo build $(CARGO_FLAGS)

# Run all tests
test: test-rust test-ui

# Run Rust tests only
test-rust:
	cargo test

# Run xa11y UI tests (requires macOS Accessibility permission)
test-ui: build setup-test
	python3 -m pytest test/ui/ -v

# Install Python test dependencies
setup-test:
	pip install -e ".[test]" --quiet

# Lint
lint:
	cargo clippy --workspace --all-targets -- -D warnings

# Format check
fmt:
	cargo fmt --check

# Type check without producing artifacts
check:
	cargo check --workspace

# Clean all build artifacts
clean:
	cargo clean
