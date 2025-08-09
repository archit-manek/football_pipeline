import subprocess
from pathlib import Path

def update_requirements():
    pyproject = Path("pyproject.toml")
    if not pyproject.exists():
        raise FileNotFoundError("❌ pyproject.toml not found in the current directory")

    print("🔁 Generating requirements.txt from pyproject.toml...")
    subprocess.run([
        "pip-compile", "pyproject.toml",
        "--output-file=requirements.txt",
        "--strip-extras"
    ], check=True)
    print("✅ requirements.txt updated.")

if __name__ == "__main__":
    update_requirements()
