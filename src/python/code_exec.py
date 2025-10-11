import subprocess
import sys
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def Run(code: str, venv_path: str = None) -> str:
    python_exec = sys.executable  
    if venv_path:
        if os.name == "nt":
            python_exec = os.path.join(venv_path, "Scripts", "python.exe")
        else:
            python_exec = os.path.join(venv_path, "bin", "python")

    process = subprocess.Popen(
        [python_exec],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    out, err = process.communicate(code)
    if process.returncode != 0:
        raise Exception(err)
  
    return out