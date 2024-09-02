import sys
from pathlib import Path

from streamlit.web import cli as stcli

app_path = Path(__file__).parent / "app.py"

if __name__ == "__main__":
    sys.argv = ["streamlit", "run", str(app_path)]
    sys.exit(stcli.main())
