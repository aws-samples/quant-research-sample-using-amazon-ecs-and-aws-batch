import sys
from pathlib import Path

# Add src/ to Python path so tests can import production modules directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
