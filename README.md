# fsds
XBRL data analysis using major accounting theories and visualization

## Setup

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env             # then edit: SEC_USER_AGENT and DB_* values
```

Run scripts from the repository root (e.g. `python revenues.py`) so the `fsds`
package is importable.
