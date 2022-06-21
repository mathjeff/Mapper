Mapper: a fast, accurate aligner for genomic sequences

Latest release version can be downloaded here: https://github.com/mathjeff/Mapper/releases/download/1.0.0/mapper-1.0.0.jar

Latest development version (which includes preliminary SAM output) can be downloaded here: https://github.com/mathjeff/Mapper/releases/download/1.1.0-beta01/mapper-1.1.0-beta01.jar

Contact:\
 Dr. Anni Zhang, MIT, anniz44@mit.edu

Usage:

  java -jar mapper.jar --reference <ref.fasta> --queries <queries.fastq> [--queries <queries2.fastq> ...] --out-vcf <out.vcf> [options]

  java -jar mapper.jar --reference <ref.fasta> --queries <queries.fastq> [--queries <queries2.fastq> ...] --out-refs-map-count <counts.txt> [options]

  java -jar mapper.jar --reference <ref.fasta> --queries <queries.fastq> [--queries <queries2.fastq> ...] --out-sam <out.sam> [options]

    Aligns genomic sequences quickly

  INPUT FILES:

    --reference <file> the reference to use. Should be in .fasta/.fa/.fna or .fastq/.fq format or a .gz of one of those formats.

    --queries <file> the reads to align to the reference. Should be in .fastq or .fasta format.
      May be specified multiple times for multiple query files

  OUTPUT FORMATS:

    Summary by position

      --out-vcf <file> output file to generate containing a description of mutation counts by position
      --vcf-exclude-non-mutations if set, the output vcf file will exclude positions where no mutations were detected
      --distinguish-query-ends <fraction> (default 0.1) In the output vcf file, we separately display which queries aligned at each position with <fraction> of the end of the query and which didn't.

    Summary by genome

      --out-refs-map-count <file> the output file to summarize the number of reads mapped to each combination of references

    Raw output

      --out-sam <file> the output file in SAM format

    --no-output skip writing an output file if none is requested

  ALIGNMENT PARAMETERS:

    --max-penalty <fraction> (default 0.1) for a match to be reported, its penalty must be no larger than this value times its length
      Setting this closer to 0 will run more quickly

    Computing the penalty of a match:

      --new-indel-penalty <penalty> (default 2) the penalty of a new insertion or deletion of length 1

      --extend-indel-penalty <penalty> (default 0.5) the penalty of an extension to an existing insertion or deletion

      --snp-penalty <penalty> (default 1) the penalty of a point mutation

    --max-num-matches <count> (default unlimited) the maximum number of positions on the reference that any query may match.
      Any query that appears to match more than this many positions on the reference will be reported as unmatched.

  OTHER:

    -v, --verbose output diagnostic information

    --verbose-alignment output verbose information about the alignment process
      This is more than what is output by --verbose

    -vv implies --verbose-alignment

    --verbosity-auto set verbosity flags dynamically and automatically based on the data and alignment

    --num-threads <count> number of threads to use at once for processing. Higher values will run more quickly on a system that has that many CPUs available.
