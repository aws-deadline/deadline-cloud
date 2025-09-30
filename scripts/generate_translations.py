#!/usr/bin/env python3
"""Generate translation files for deadline GUI."""

import os
import subprocess
import sys
from pathlib import Path

def main():
    """Generate .ts files from Python source files."""
    
    # Find the project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    src_dir = project_root / "src" / "deadline" / "client" / "ui"
    translations_dir = src_dir / "translations"
    
    # Create translations directory if it doesn't exist
    translations_dir.mkdir(exist_ok=True)
    
    # Find all Python files in the UI directory
    py_files = []
    for py_file in src_dir.rglob("*.py"):
        py_files.append(str(py_file))
    
    if not py_files:
        print("No Python files found in UI directory")
        return 1
    
    # Generate English translation file
    ts_file = translations_dir / "deadline_en.ts"
    
    try:
        # Use pylupdate6 if available, otherwise try pylupdate5
        cmd = ["pylupdate6"] + py_files + ["-ts", str(ts_file)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            # Try pylupdate5 as fallback
            cmd[0] = "pylupdate5"
            result = subprocess.run(cmd, capture_output=True, text=True)
            
        if result.returncode != 0:
            print(f"Error running pylupdate: {result.stderr}")
            return 1
            
        print(f"Generated translation file: {ts_file}")
        
        # Compile the .ts file to .qm
        qm_file = translations_dir / "deadline_en.qm"
        cmd = ["lrelease", str(ts_file), "-qm", str(qm_file)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Error running lrelease: {result.stderr}")
            return 1
            
        print(f"Compiled translation file: {qm_file}")
        
    except FileNotFoundError as e:
        print(f"Qt translation tools not found: {e}")
        print("Please install Qt development tools (pylupdate6/pylupdate5 and lrelease)")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
