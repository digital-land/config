#!/bin/sh

github="https://raw.githubusercontent.com/digital-land/"

csvcut -c collection specification/collection.csv | tail -n +2 |
while read collection
do
    for file in \
	column.csv\
	combine.csv\
	concat.csv\
	convert.csv\
	default.csv\
	default-value.csv\
	patch.csv\
	skip.csv\
	transform.csv\
	filter.csv\
	lookup.csv
    do
        dir=pipeline/$collection
        path=pipeline/$collection/$file
        if [ ! -f $path ] ; then
            mkdir -p $dir
            set -x
            wget -q -O $path "$github$collection-collection/main/pipeline/$file"
            set +x
        fi
    done
done

