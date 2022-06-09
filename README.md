Mapper: a fast, accurate aligner for genomic sequences

Trial version can be downloaded here: https://github.com/mathjeff/Mapper/releases/download/1.0.0/mapper-1.0.0.jar

Usage:\
  java -jar mapper1.jar --reference <reference.fasta> --queries <queries.fastq> [--queries <queries2.fastq> ...] --out-vcf <out.vcf> [options]

Contact:\
 Dr. Anni Zhang, MIT, anniz44@mit.edu

To build:\
  ./gradlew build\
  java -jar build/libs/mapper.jar
