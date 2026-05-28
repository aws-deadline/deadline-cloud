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

.PHONY: build build-rust build-python test test-rust test-ui test-bindings setup setup-rust setup-python lint lint-python fmt fmt-python check clean

# ── Build ──

build: build-rust build-python

build-rust:
	cargo build $(CARGO_FLAGS)

build-python:
	maturin develop --quiet

# ── Test ──

test: test-rust test-ui test-bindings

test-rust:
	cargo test

test-ui: build-python
	$(PYTHON) -m pytest pytests/ui_accessibility/ -v

test-bindings: build-python
	$(PYTHON) -m pytest pytests/bindings/ -v

# ── Setup (once after clone or after dependency changes) ──

setup: setup-rust setup-python

setup-rust:
	cargo install cargo-insta cargo-deny cargo-outdated cargo-bloat

setup-python:
	$(PIP) install -e ".[test]" --quiet

# ── Quality ──

lint:
	cargo clippy --workspace --all-targets -- -D warnings

lint-python:
	ruff check gui/ pytests/

fmt:
	cargo fmt --check

fmt-python:
	ruff format gui/ pytests/

check:
	cargo check --workspace

# ── Clean ──

clean:
	cargo clean
