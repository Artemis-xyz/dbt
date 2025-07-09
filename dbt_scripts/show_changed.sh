# Obtain DBT Manifest
wget https://artemis-xyz.github.io/dbt/manifest.json

changed_models=$(dbt ls --select state:modified+1 --resource-type model --state . --quiet --target prod)

echo "\nCHANGED MODELS:"
echo "$changed_models"
echo "\n"
echo "$changed_models" | while IFS= read -r model; do
    echo "SHOWING $model"
    dbt show --select "$model" --limit 10 || true
done