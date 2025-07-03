# Assumtions:
# 1. Everything is stored in the projects folder not staging

# Check if exactly one argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <project_name>"
    exit 1
fi

PROJECT_NAME="$1"

INPUT_FOLDER="models/projects/$PROJECT_NAME"
OUTPUT_FOLDER="models/_complete_asset_models/$PROJECT_NAME"

mkdir -p "$OUTPUT_FOLDER"

BACKUP_FILE="materialized_backup.txt"

echo "Backing up original materialized settings with filenames..."
grep -r 'materialized="' "$INPUT_FOLDER" | sed 's/^\(.*\):.*materialized="\(.*\)".*/\1:\2/' > "$BACKUP_FILE"

echo "Changing all materialized settings to ephemeral..."
find "$INPUT_FOLDER" -type f -name "*.sql" | while read -r file; do
    sed -i '' 's/materialized="table"/materialized="ephemeral"/' "$file"
    sed -i '' 's/materialized="incremental"/materialized="ephemeral"/' "$file"
done

CORE_FOLDER_PATH="$INPUT_FOLDER/core"
echo "Running dbt compile for each file..."
find "$CORE_FOLDER_PATH" -type f -name "*.sql" | while read -r file; do
    base_filename=$(basename "$file" .sql)

    output_file="$OUTPUT_FOLDER/${base_filename}__complete__.sql"

    dbt compile --select "$file" > "$output_file" --target prod
    # Remove unwanted log lines from the first 7 lines of the output file
    head -n 7 "$output_file" | sed '/^\[0m/d' | sed '/^Found /d' | sed '/^Concurrency: /d' > "${output_file}.tmp"
    tail -n +8 "$output_file" >> "${output_file}.tmp"
    mv "${output_file}.tmp" "$output_file"

    echo "Compiled and created: $output_file"
done

#Restore the original `materialized` settings
echo "Restoring original materialized settings..."
while IFS=: read -r filepath materialized_value; do
    sed -i '' "s/materialized=\"ephemeral\"/materialized=\"$materialized_value\"/" "$filepath"
done < "$BACKUP_FILE"

Clean up the backup file
rm "$BACKUP_FILE"
echo "All files restored to original materialized settings."