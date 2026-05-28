PROFILE ?= debug
CARGO_FLAGS := $(if $(filter release,$(PROFILE)),--release,)
TARGET_DIR := target/$(PROFILE)
BIN_NAME := deadline

# Use .venv if it exists and no venv is already active
VENV_DIR := .venv
ifdef VIRTUAL_ENV
  PYTHON := python3
  PIP := pip
  RUFF := ruff
else ifneq (,$(wildcard $(VENV_DIR)/bin/python3))
  PYTHON := $(VENV_DIR)/bin/python3
  PIP := $(VENV_DIR)/bin/pip
  RUFF := $(VENV_DIR)/bin/ruff
else
  PYTHON := python3
  PIP := pip
  RUFF := ruff
endif

.PHONY: build build-rust build-python test test-rust test-python test-ui test-bindings setup setup-rust setup-python lint lint-rust lint-python fmt fmt-rust fmt-python check clean

# ── Build ──

build: build-rust build-python

build-rust:
	cargo build $(CARGO_FLAGS)

build-python:
	maturin develop --quiet

# ── Test ──

test: test-rust test-python

test-rust:
	cargo test

test-python: test-ui test-bindings

test-ui: build-python
	$(PYTHON) -m pytest pytests/ui_accessibility/ -v

test-bindings: build-python
	$(PYTHON) -m pytest pytests/bindings/ -v

# ── Setup (once after clone or after dependency changes) ──

setup: setup-rust setup-python

setup-rust:
	cargo install cargo-insta cargo-deny cargo-outdated cargo-bloat

setup-python:
	python3 -m venv $(VENV_DIR)
	$(VENV_DIR)/bin/pip install -e ".[test,dev]" --quiet

# ── Quality ──

lint: lint-rust lint-python
	cargo fmt --check
	$(RUFF) format --check gui/ pytests/

lint-rust:
	cargo clippy --workspace --all-targets -- -D warnings

lint-python:
	$(RUFF) check gui/ pytests/

fmt: fmt-rust fmt-python

fmt-rust:
	cargo fmt

fmt-python:
	$(RUFF) format gui/ pytests/
	$(RUFF) check --fix gui/ pytests/

check:
	cargo check --workspace

# ── Clean ──

clean:
	cargo clean
