from pathlib import Path
import subprocess, json
import re
from typing import Iterator, Dict, Any

def extract_sql_columns(sql_file_path: str) -> list[str]:
    """Return list of column names found in the main SELECT of a model, handling incremental logic."""
    def _strip_comments(s: str) -> str:
        s = re.sub(r'--.*?$', '', s, flags=re.MULTILINE)
        return re.sub(r'/\*.*?\*/', '', s, flags=re.DOTALL)

    with open(sql_file_path, "r") as fh:
        cleaned = _strip_comments(fh.read())

    matches = re.findall(r"select\s+(.*?)\s+from", cleaned,
                         flags=re.IGNORECASE | re.DOTALL)
    if not matches:
        return []

    # Check if the last SELECT contains incremental logic in the SELECT clause itself
    last_select = matches[-1]
    
    # Look for specific incremental patterns that indicate this is an incremental model
    # Common patterns: max(this.date), max(date), etc.
    incremental_patterns = [
        r"\bmax\s*\(\s*this\.\w+\s*\)",  # max(this.column)
        r"\bmax\s*\(\s*\w+\.\w+\s*\)",   # max(table.column)
        r"\bmax\s*\(\s*\w+\s*\)\s*from\s+\w+",  # max(column) from table
    ]
    
    is_incremental = False
    for pattern in incremental_patterns:
        if re.search(pattern, last_select, re.IGNORECASE):
            is_incremental = True
            break
    
    # Use second-to-last SELECT if incremental logic is detected in the SELECT clause, otherwise use last SELECT
    if is_incremental and len(matches) > 1:
        select_clause = matches[-2]  # Use second-to-last SELECT
    else:
        select_clause = matches[-1]  # Use last SELECT
    
    cols: set[str] = set()

    for line in select_clause.splitlines():
        line = line.strip().rstrip(",")
        if not line:
            continue

        # 1)  … AS alias
        if m := re.search(r"\s+as\s+(\w+)", line, flags=re.IGNORECASE):
            cols.add(m.group(1))
            continue
        # 2)  table.column or just column
        if m := re.search(r"(?:\w+\.)?(\w+)", line):
            cols.add(m.group(1))

    return sorted(cols)

def get_dbt_root() -> str:
    """Find the dbt project root directory by looking for dbt_project.yml"""
    import os
    cwd = os.getcwd()
    while cwd != "/":
        if os.path.exists(os.path.join(cwd, "dbt_project.yml")):
            return cwd
        cwd = os.path.dirname(cwd)
    raise RuntimeError("dbt_project.yml not found while ascending directories.")


class ModelMetadata:
    """
    Yields dicts with model metadata – name, path, columns, dtypes, table_type.
    """
    def __init__(self, project: str):
        self.project = project
        self.dbt_root = get_dbt_root()
        self.compiled_dir = (
            Path(self.dbt_root)
            / "target"
            / "compiled"
            / "artemis_dbt"
            / "models"
            / "projects"
            / project
            / "core"
        )

    # ────────────────────────────────────────────────────────────
    #  Public iterator
    # ────────────────────────────────────────────────────────────
    def iter_models(self) -> Iterator[Dict[str, Any]]:
        for sql in self.compiled_dir.glob("*.sql"):
            cols = extract_sql_columns(sql)
            dtypes = self._fetch_column_types(sql.stem)
            table_type = self._infer_type(cols, sql.stem)
            
            # Add subtype based on table type
            ez_subtype = None
            fact_subtype = None
            if table_type == "ez":
                ez_subtype = self._get_ez_subtype(sql.stem)
            elif table_type == "fact":
                fact_subtype = self._get_fact_subtype(sql.stem)
            
            yield {
                "name": sql.stem,
                "path": sql,
                "columns": cols,
                "dtypes": dtypes,
                "table_type": table_type,
                "ez_subtype": ez_subtype,
                "fact_subtype": fact_subtype,
            }

    # ────────────────────────────────────────────────────────────
    #  Helpers
    # ────────────────────────────────────────────────────────────
    def _fetch_column_types(self, model_name: str) -> dict[str, str]:
        """
        Quick Snowflake INFORMATION_SCHEMA lookup.
        Adjust `database.schema` to match yours.
        """
        try:
            query = f"""
            select column_name, data_type
            from information_schema.columns
            where table_name ilike '{model_name}'
            """
            # change to your preferred DB-API call if needed
            res = subprocess.check_output(["snowflake", "-q", query])
            # expecting TSV "COLUMN_NAME<TAB>DATA_TYPE\n…"
            return dict(line.split('\t') for line in res.decode().splitlines())
        except Exception:
            return {}

    def _infer_type(self, cols: list[str], model_name: str) -> str:
        # Classify based on model name prefix
        if model_name.startswith("ez"):
            return "ez"
        elif model_name.startswith("fact"):
            return "fact"
        return "other"
    
    def _get_ez_subtype(self, model_name: str) -> str:
        """Determine EZ table subtype based on model name."""
        if model_name.endswith("_by_chain"):
            return "ez_by_chain"
        elif model_name.endswith("_by_token"):
            return "ez_by_token"
        elif model_name.endswith("_by_app"):
            return "ez_by_app"
        else:
            return "ez_general"
    
    def _get_fact_subtype(self, model_name: str) -> str:
        """Determine FACT table subtype based on model name."""
        if "rolling" in model_name:
            return "fact_rolling"
        elif "daily" in model_name:
            return "fact_daily"
        elif "hourly" in model_name:
            return "fact_hourly"
        elif "weekly" in model_name:
            return "fact_weekly"
        elif "monthly" in model_name:
            return "fact_monthly"
        else:
            return "fact_general"