#!/usr/bin/env bash

# golang packages that will be used for either testing or will be assessed for coverage
pck1=github.com/zillow/zkafka
pck2=$pck1/test

topdir=$(pwd)

# binary result filepaths
root_res=$topdir/root.res
source_res=$topdir/source.res

# go cover formatted files
root_out=$topdir/root.out
source_out=$topdir/source.out

cover_out=$topdir/cover.out

function quit() {
  echo "Error in coverage.sh. Stopping processing";
  exit;
}
# change to example directory for execution (because it uses hardcoded filepaths, and the testable
# examples don't work when executed outside of that directory
go test -c -coverpkg=$pck1 -covermode=atomic -o "$root_res" $pck1
# convert binary to go formatted
go tool test2json -t "$root_res" -test.v -test.coverprofile "$root_out"

go test -c -coverpkg=$pck1 -covermode=atomic -o "$source_res" $pck2
go tool test2json -t "$source_res" -test.v -test.coverprofile "$source_out"

# delete aggregate file
rm "$cover_out"

# combine the results (the first line should be omitted on subsequent appends)
cat "$root_out" >> "$cover_out"
tail -n+2 "$source_out" >> "$cover_out"

# print aggregated results
go tool cover -func="$cover_out"
go tool cover -html="$cover_out" -o cover.html
