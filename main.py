import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.runner import runner

if __name__ == '__main__':
    runner()