PROFILE ?= debug
CARGO_FLAGS := $(if $(filter release,$(PROFILE)),--release,)
TARGET_DIR := target/$(PROFILE)
BIN_NAME := deadline

.PHONY: all build develop wheel test test-rust test-python lint fmt check clean setup-tools

# Install required and recommended Cargo tools
setup-tools:
	cargo install cargo-insta cargo-deny cargo-outdated cargo-bloat

# Default: build everything and install into .venv
all: develop

# Build Rust workspace (all crates)
build:
	cargo build $(CARGO_FLAGS)

# Build + install into .venv (local development)
develop: build
	@mkdir -p deadline.data/scripts
	cp $(TARGET_DIR)/$(BIN_NAME) deadline.data/scripts/$(BIN_NAME)
	maturin develop

# Build release wheel for distribution
wheel:
	$(MAKE) build PROFILE=release
	@mkdir -p deadline.data/scripts
	cp target/release/$(BIN_NAME) deadline.data/scripts/$(BIN_NAME)
	maturin build --release

# Run all tests
test: test-rust test-python

# Run Rust tests only
test-rust:
	cargo test

# Run Python GUI tests only
test-python:
	pytest gui/tests/ -v

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
	rm -rf deadline.data
	rm -f gui/deadline/_native.abi3.so
