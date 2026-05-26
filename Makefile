PROFILE ?= debug
CARGO_FLAGS := $(if $(filter release,$(PROFILE)),--release,)
TARGET_DIR := target/$(PROFILE)
BIN_NAME := deadline

# Use .venv if it exists and no venv is already active
VENV_DIR := .venv
ifdef VIRTUAL_ENV
  PYTHON := python3
  PIP := pip
else ifneq (,$(wildcard $(VENV_DIR)/bin/python3))
  PYTHON := $(VENV_DIR)/bin/python3
  PIP := $(VENV_DIR)/bin/pip
else
  PYTHON := python3
  PIP := pip
endif

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
test: test-rust test-ui test-bindings

# Run Rust tests only
test-rust:
	cargo test

# Run xa11y UI tests (requires macOS Accessibility permission)
test-ui: build setup-test
	$(PYTHON) -m pytest pytests/ui_accessibility/ -v

# Run PyO3 binding tests
test-bindings: setup-test
	maturin develop --quiet && $(PYTHON) -m pytest pytests/bindings/ -v

# Install Python test dependencies
setup-test:
	$(PIP) install -e ".[test]" --quiet

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
