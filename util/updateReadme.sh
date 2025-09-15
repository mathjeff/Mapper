#!/bin/bash
set -e
cd "$(dirname $0)"

mapperPath=../build/libs/x-mapper.jar
tempReadme=../build/README.md
helpPath=../build/help.txt
destReadme=../README.md

# clear file
rm -f "$tempReadme"

# start header
echo "# X-Mapper: a fast, accurate aligner for genomic sequences
" >> "$tempReadme"

# extract latest download link
grep "Download the latest release version here" "$destReadme" >> "$tempReadme"

# finish header
echo '
Also available as `x-mapper` in Bioconda - see https://bioconda.github.io/recipes/x-mapper/README.html

Read about the algorithm, plus benchmarking and application in the publication here: https://genomebiology.biomedcentral.com/articles/10.1186/s13059-024-03473-7

If you already have aligned sequences and just want to identify genetic variants, see [QuickVariants](https://github.com/caozhichongchong/QuickVariants).

Contact:\
 Dr. Anni Zhang, caozhichongchong at gmail dot com' >> "$tempReadme"

# write usage
# remove line feed characters
# skip writing the version line
# replace the shorter project name Mapper in the help with the more easily searchable name X-Mapper in the readme
# don't replace X-Mapper with X-X-Mapper
# mark the Usage line as important
cat "$helpPath" | tr -d '\r' | grep -v "Mapper version" | sed 's/Mapper/X-Mapper/g' | sed 's/X-X-Mapper/X-Mapper/g' | sed 's/^Usage/## Usage/' | sed 's/abbr. X-Mapper/abbr. Mapper/' >> "$tempReadme"

# write suffix

echo "To try a simple example, see examples/test.sh

To see more demos, see https://github.com/mathjeff/Mapper-Demos

To make changes to X-Mapper, see [DEVELOPING.md](DEVELOPING.md)

## If you're working on a bioinformatics project and would be interested in some consulting help, check out our website at https://genomiverse.net/ !" >> "$tempReadme"

# update README
cp "$tempReadme" "$destReadme"
