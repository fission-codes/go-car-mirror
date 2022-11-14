#!/bin/bash

fuzzTime=${1:-10000x}

files=$(grep -r --include='**_test.go' --files-with-matches 'func Fuzz' .)

if [ -z "$files" ]; then
  echo "No fuzz tests found."
  exit 0
fi

for file in ${files}
do
	funcs=$(egrep -o '(Fuzz\w+)' $file | sort -u)
	for func in ${funcs}
	do
		echo "Fuzzing $func in $file"
		parentDir=$(dirname $file)
		go test $parentDir -run=$func -fuzz=$func -fuzztime=${fuzzTime}
	done
done
