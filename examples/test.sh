#!/bin/bash
set -e
cd "$(dirname $0)"

function buildMapper() {
  cd ..
  ./gradlew assemble
  cd -
}
buildMapper

function runMapper() {
  mkdir -p out
  java -jar ../build/libs/x-mapper.jar --reference reference.fasta  --queries queries.fasta --out-sam out/out.sam --out-vcf out/out.vcf --out-refs-map-count out/ref-counts.txt --out-unaligned out/out-unaligned.fasta
}
runMapper

function outputInstructions() {
  echo "Now look at the results in out/"
}
