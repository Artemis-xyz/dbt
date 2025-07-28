import sys
from pathlib import Path

from .metadata import ModelMetadata
from .rules import RuleEngine
from .renderer import YamlRenderer

ROOT = Path(__file__).parent

def main():
    if len(sys.argv) < 2:
        sys.exit("Usage: python -m dbt_scripts.generate_tests <project> [profile]")

    project = sys.argv[1]
    profile = sys.argv[2] if len(sys.argv) > 2 else "ez_tables"

    meta = ModelMetadata(project)
    engine = RuleEngine(profile_path=ROOT / "profiles" / f"{profile}.yml")
    renderer = YamlRenderer()

    for model in meta.iter_models():
        renderer.write_tests(model, engine.generate_tests(model))

if __name__ == "__main__":
    main() 