#!/bin/bash
# Get the list of changed SQL files
CHANGED_FILES=$(git diff --cached --name-only --diff-filter=ACM | grep '\.sql$')

if [ -z "$CHANGED_FILES" ]; then
  echo "No changed SQL files to lint"
  exit 0
fi

# Run SQLFluff on each changed SQL file
for file in $CHANGED_FILES; do
  sqlfluff fix "$file"
  if [ $? -ne 0 ]; then
    echo "SQLFluff found issues in $file"
    exit 1
  fi
done

exit 0
