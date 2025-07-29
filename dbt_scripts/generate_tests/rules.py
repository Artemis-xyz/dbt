import yaml
import re
from typing import Any, Dict, List

class RuleEngine:
    def __init__(self, profile_path: str):
        self.profile = yaml.safe_load(open(profile_path))

    # ────────────────────────────────────────────────────────────
    #  Core public method
    # ────────────────────────────────────────────────────────────
    def generate_tests(self, model: Dict[str, Any]) -> List[Dict[str, Any]]:
        tests = []
        
        # Generate table-level tests
        for rule in self.profile["rules"]:
            if self._matches(rule["when"], model):
                severity = rule.get("severity", "error")
                for t in rule["tests"]:
                    tests.append({"severity": severity, "definition": t})
        
        # Generate column-level tests
        if "column_rules" in self.profile:
            for rule in self.profile["column_rules"]:
                if self._matches_column_rule(rule["when"], model):
                    severity = rule.get("severity", "error")
                    for column in rule["columns"]:
                        for t in rule["tests"]:
                            # Replace column placeholder with actual column name
                            test_def = self._replace_column_placeholder(t, column["name"])
                            tests.append({"severity": severity, "definition": test_def})
        
        return tests

    # ────────────────────────────────────────────────────────────
    #  Matchers
    # ────────────────────────────────────────────────────────────
    def _matches(self, when: Dict[str, Any], model: Dict[str, Any]) -> bool:
        # table_type filter
        if "table_type" in when and when["table_type"] != model["table_type"]:
            return False
        
        # ez_subtype filter
        if "ez_subtype" in when and when["ez_subtype"] != model.get("ez_subtype"):
            return False
        
        # fact_subtype filter
        if "fact_subtype" in when and when["fact_subtype"] != model.get("fact_subtype"):
            return False

        # column pattern / name filter
        if "columns" in when:
            col_rules = when["columns"]
            for rule in col_rules:
                if "name" in rule and rule["name"] not in model["columns"]:
                    return False
                if "pattern" in rule:
                    pat = re.compile(rule["pattern"])
                    if not any(pat.match(c) for c in model["columns"]):
                        return False
        return True

    def _matches_column_rule(self, when: Dict[str, Any], model: Dict[str, Any]) -> bool:
        """Match rules for column-specific tests"""
        # Check if any of the specified columns exist in the model
        if "columns" in when:
            for col_rule in when["columns"]:
                if "name" in col_rule and col_rule["name"] in model["columns"]:
                    return True
                if "pattern" in col_rule:
                    pat = re.compile(col_rule["pattern"])
                    if any(pat.match(c) for c in model["columns"]):
                        return True
        return False

    def _replace_column_placeholder(self, test_def: Dict[str, Any], column_name: str) -> Dict[str, Any]:
        """Replace {column} placeholder with actual column name"""
        import copy
        test_copy = copy.deepcopy(test_def)
        
        def replace_in_dict(d):
            for key, value in d.items():
                if isinstance(value, dict):
                    replace_in_dict(value)
                elif isinstance(value, str):
                    d[key] = value.replace("{column}", column_name)
        
        replace_in_dict(test_copy)
        return test_copy 