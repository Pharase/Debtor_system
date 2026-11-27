"""Shared settings for the debtor system: .env loading, database path and legacy column names.

All settings live in .env next to this file (copy .env.example). Nothing here is machine-specific.
"""
import os
import shutil
import sqlite3

HERE = os.path.dirname(os.path.abspath(__file__))


def load_env(path=os.path.join(HERE, ".env")):
    """Read KEY=VALUE lines from .env into the environment (no extra package needed)."""
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                if "=" in line and not line.lstrip().startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip())


load_env()

def _path(value):
    """Relative paths in .env count from this folder, not from wherever the script was started."""
    return value if os.path.isabs(value) else os.path.join(HERE, value)


DB_PATH = _path(os.environ.get("DEBTOR_DB_PATH") or os.path.join("database", "Debtor_system.db"))
DB_FOLDER = os.path.dirname(DB_PATH)
TRANSACTION_FOLDER = _path(os.environ.get("TRANSACTION_FOLDER") or "data")
LEGACY_COLUMN_PREFIX = os.environ.get("LEGACY_COLUMN_PREFIX", "").lower()


def neutral_name(column):
    """<prefix>_code / <prefix>code -> account_code, <prefix>xyz -> xyz. Other names are unchanged."""
    name = str(column)
    if not LEGACY_COLUMN_PREFIX or not name.lower().startswith(LEGACY_COLUMN_PREFIX):
        return name
    rest = name[len(LEGACY_COLUMN_PREFIX):].lstrip("_")
    if rest.lower().startswith("code"):
        return "account_" + rest.lower()
    return "account_id" if rest.lower() == "id" else rest


def normalize_columns(df):
    """Rename legacy headers in a DataFrame read from an Excel export."""
    return df.rename(columns=neutral_name)


def migrate_db(path):
    """Rename legacy column names in every table once. A copy of the database is saved first."""
    if not LEGACY_COLUMN_PREFIX or not os.path.exists(path):
        return
    conn = sqlite3.connect(path)
    try:
        renames = []
        for (table,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table'"):
            for col in conn.execute(f'PRAGMA table_info("{table}")'):
                new = neutral_name(col[1])
                if new != col[1]:
                    renames.append((table, col[1], new))
        if not renames:
            return
        backup = path + ".before-rename.bak"
        if not os.path.exists(backup):
            shutil.copy2(path, backup)
        for table, old, new in renames:
            conn.execute(f'ALTER TABLE "{table}" RENAME COLUMN "{old}" TO "{new}"')
        conn.commit()
        print(f"Renamed {len(renames)} legacy column(s) in {os.path.basename(path)} (backup: {os.path.basename(backup)})")
    finally:
        conn.close()


_migrated = set()


def connect(path=None):
    """sqlite3 connection with legacy column names migrated on first use."""
    path = path or DB_PATH
    if path not in _migrated:
        migrate_db(path)
        _migrated.add(path)
    return sqlite3.connect(path)
