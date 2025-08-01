from pathlib import Path
from .metadata import ModelMetadata
from .rules import RuleEngine
from .renderer import YamlRenderer

def generate_tests_for_project(project_name: str, profile: str = "ez_tables"):
    """
    Generate tests for all models in a project.
    
    Args:
        project_name: Name of the project to generate tests for
        profile: Profile name to use (default: "ez_tables")
    
    Returns:
        dict: Summary of results with success count and any errors
    """
    results = {
        "project": project_name,
        "models_processed": 0,
        "models_with_tests": 0,
        "errors": []
    }
    
    try:
        # Initialize components
        meta = ModelMetadata(project_name)
        engine = RuleEngine(profile_path=Path(__file__).parent / "profiles" / f"{profile}.yml")
        renderer = YamlRenderer()
        
        # Process each model
        for model in meta.iter_models():
            try:
                results["models_processed"] += 1
                tests = engine.generate_tests(model)
                
                if tests:
                    renderer.write_tests(model, tests)
                    results["models_with_tests"] += 1
                    
            except Exception as e:
                error_msg = f"Error processing model {model.get('name', 'unknown')}: {str(e)}"
                results["errors"].append(error_msg)
                
    except Exception as e:
        results["errors"].append(f"Error initializing test generation: {str(e)}")
    
    return results

def main():
    """Command-line entry point - kept for backward compatibility"""
    import sys
    
    if len(sys.argv) < 2:
        sys.exit("Usage: python -m dbt_scripts.generate_tests <project> [profile]")

    project = sys.argv[1]
    profile = sys.argv[2] if len(sys.argv) > 2 else "ez_tables"
    
    results = generate_tests_for_project(project, profile)
    
    # Print results
    print(f"Processed {results['models_processed']} models for project '{results['project']}'")
    print(f"Generated tests for {results['models_with_tests']} models")
    
    if results['errors']:
        print(f"Encountered {len(results['errors'])} errors:")
        for error in results['errors']:
            print(f"  - {error}")
        sys.exit(1)
    else:
        print("✅ All tests generated successfully!")

# For backward compatibility
if __name__ == "__main__":
    main()
