#!/bin/bash

# Get the directory where the script is located
script_dir="$(dirname "$0")"

# List of files to exclude
exclude_files=("README.md" "go.sum" "go.mod" "output.txt" "Makefile" "cat-all-code.sh" "metrics_aggregation_processor_test.go")

# Construct the find command with the exclusions
find_cmd="find \"$script_dir\" -type f"

for file in "${exclude_files[@]}"; do
    find_cmd="$find_cmd ! -name \"$file\""
done

# Define the output file
output_file="$script_dir/output.txt"

# Empty the output file if it exists
> "$output_file"

# Process each file
while IFS= read -r -d '' file; do
    # Print filename and content wrapped with ```
    {
        echo "$file"
        echo '```'
        cat "$file"
        echo 
        echo '```'
    } >> "$output_file"
done < <(eval "$find_cmd" -print0)

# Add a newline at the end (optional)
echo >> "$output_file"