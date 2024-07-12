if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <directory>"
    exit 1
fi

directory=$1
project=$(basename "$directory")

output_file="$directory/__${project}__metadata.yml"

echo "models:" >> "$output_file"

find "$directory" -type f -name "*.sql" -exec basename {} \; | while read -r filename; do
    filename_without_extension="${filename%.sql}"
    echo "  - name: $filename_without_extension" >> "$output_file"
    echo "    config:" >> "$output_file"
    echo "      meta:" >> "$output_file"
    echo "        dagster:" >> "$output_file"
    echo "          group: $project" >> "$output_file"
done
