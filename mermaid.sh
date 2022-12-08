#!/usr/bin/env bash

if [ $# -lt 1 ]; then
  echo "Error: No file argument provided" >&2
  exit 1
fi

file=$1

DATE=$(date +"%Y-%m-%dT%H:%M:%SZ")
MY_TMP=$(mktemp -d "/tmp/mermaid.$DATE.XXXXXX") || die "could not 'mktemp -d /tmp/mermaid.$DATE.XXXXXX'"

# First we need to convert the file to a single list of json objects, so we can use jq on the entire file instead of just line by line
# TODO
# First line [.  Last line ].
# Add commas to end of each log line.

mermaid_file=$MY_TMP/$file.mermaid
json_file=$MY_TMP/$file.json
cat $file | grep mermaid/mermaid > $mermaid_file

json_lines=()

# Read each line of the file
while read line; do
  # Add the line to the array
  json_lines+=("$line")
done < $mermaid_file

# Convert the array to a JSON list using jq
entities=$(jq -sr '.[] | .entity' <<< "${json_lines[@]}" | sort -u)

for e in $entities; do
  echo "e=$e"
done

# Get unique entities so we can separate their diagrams
# while read line; do
#   echo "$line" | jq -r '.entity' | sort -u
# done < $mermaid_file > $entities_file

# for e in $entities; do
#   echo "# $e"
#   while read line; do
#     if echo "$line" | jq -e 'has("entity") and .entity == "$entity"'; then
#       echo "$line" | jq -r '.msg'
#     fi
#   done < $mermaid_file
# done

