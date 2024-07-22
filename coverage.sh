#!/usr/bin/env bash

# golang packages that will be used for either testing or will be assessed for coverage
pck1=gitlab.zgtools.net/devex/archetypes/gomods/zstreams/v4
pck2=$pck1/test

topdir=$(pwd)

# binary result filepaths
root_res=$topdir/root.res
source_res=$topdir/source.res

# go cover formatted files
root_out=$topdir/root.out
source_out=$topdir/source.out

omni_out=$topdir/omni.out

function quit() {
  echo "Error in coverage.sh. Stopping processing";
  exit;
}
# change to example directory for execution (because it uses hardcoded filepaths, and the testable
# examples don't work when executed outside of that directory
go test -c -coverpkg=$pck1 -covermode=atomic -o "$root_res" -tags integration $pck1
# convert binary to go formatted
go tool test2json -t "$root_res" -test.v -test.coverprofile "$root_out"

go test -c -coverpkg=$pck1 -covermode=atomic -o "$source_res" -tags integration $pck2
go tool test2json -t "$source_res" -test.v -test.coverprofile "$source_out"

# delete aggregate file
rm "$omni_out"

# combine the results (the first line should be omitted on subsequent appends)
cat "$root_out" >> "$omni_out"
tail -n+2 "$source_out" >> "$omni_out"

# print aggregated results
go tool cover -func="$omni_out"

# we need to create a cobertura.xml file (this is used for code coverage visualization)
# download the tool to convert gocover into covertura
go install github.com/boumenot/gocover-cobertura@latest
gocover-cobertura < $omni_out > coverage.tmp.xml
# the cobertura generated file has two issues. Its source array doesn't include the curdir and
# the class node filepaths aren't relative to the root.
# We'll run two commands
# 1. Remove the prefixed go.mod package name from filenames inside of the cobertura with a brute force replace with empty string
# 2. Add the workingdirectory to the sources array using a find replace (search for sources node, and replace with sources node but new workdir source nod)
pkg=gitlab.zgtools.net/devex/archetypes/gomods/zstreams
sed "s|$pkg/||" coverage.tmp.xml | sed "s|<sources>|<sources>\n<source>$(pwd)</source>|"> coverage.xml