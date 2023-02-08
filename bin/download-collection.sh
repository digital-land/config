#!/bin/sh

set -e

s3="https://files.planning.data.gov.uk/"

csvcut -c collection specification/collection.csv | tail -n +2 |
while read collection
do
    for file in endpoint.csv source.csv old-resource.csv
    do
        dir=collection/$collection
        path=collection/$collection/$file
        if [ ! -f $path ] ; then
            mkdir -p $dir
            set -x
            curl -qsfL $flags --retry 3 -o $path "$s3$collection-collection/collection/$file"
            set +x
        fi
    done
done

