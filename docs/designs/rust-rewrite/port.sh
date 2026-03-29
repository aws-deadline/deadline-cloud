#!/bin/bash
# Start a rust-rewrite porting session with the correct context loaded.
cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"

exec kiro-cli chat \
  "Read docs/designs/rust-rewrite/workflow.md and the Progress table in docs/designs/rust-rewrite/README.md. Follow the workflow starting from Step 0."
