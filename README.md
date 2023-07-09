Mapper: a fast, accurate aligner for genomic sequences

Latest release version can be downloaded here: https://github.com/mathjeff/Mapper/releases/download/1.0.0/mapper-1.0.0.jar

Latest development version (which includes preliminary SAM output) can be downloaded here: https://github.com/mathjeff/Mapper/releases/download/1.1.0-beta05/mapper-1.1.0-beta05.jar

Contact:\
 Dr. Anni Zhang, MIT, anniz44@mit.edu


Usage:
  java -jar mapper.jar [--out-vcf <out.vcf>] [--out-sam <out.sam>] [--out-refs-map-count <counts.txt>] [--out-unaligned <unaligned.fastq>] --reference <ref.fasta> --queries <queries.fastq> [options]
  java -jar mapper.jar [--out-vcf <out.vcf>] [--out-sam <out.sam>] [--out-refs-map-count <counts.txt>] [--out-unaligned <unaligned.fastq>] --reference <ref.fasta> --paired-queries [--spacing <expected> <distancePerPenalty>]<queries.fastq> <queries2.fastq> [options]

    Aligns genomic sequences quickly

  INPUT:

    --reference <file> the reference to use. Should be in .fasta/.fa/.fna or .fastq/.fq format or a .gz of one of those formats.

    --queries <file> the reads to align to the reference. Should be in .fastq or .fasta format.
      May be specified multiple times for multiple query files
    --paired-queries <file1> <file2> [--spacing <mean> <distancePerPenalty>] Illumina-style paired-end reads to align to the reference. For each read <r1> in <file1> and the corresponding <r2> in <file2>, the alignment position of <r1> is expected to be slightly before the alignment position of the reverse complement of <r2>.
      Each file should be in .fastq/.fasta format
      May be specified multiple times for multiple query files

      --spacing <expected> <distancePerPenalty> (default: 100 50)
        Any query alignment whose inner distance deviates from <expected> has an additional penalty added.
        That additional penalty equals (the difference between the actual distance and <expected>) divided by <distancePerPenalty>, unless the two query sequence alignments would overlap, in which case the additional penalty is 0.

      --no-infer-ancestors
        By default, if Mapper detects several highly similar sections of the reference genome, it will infer that they likely shared a common ancestor in the past and will lower the penalty of an alignment that mismatches the given reference but matches the inferred common ancestor.
        This flag disables that behavior.

    --split-queries-past-size <size> Any queries longer than <size> will be split into smaller queries.
      THIS OPTION IS A TEMPORARY EXPERIMENT FOR DETECTING REARRANGEMENTS IN LONG READS.

  OUTPUT FORMATS:

    Summary by position

      --out-vcf <file> output file to generate containing a description of mutation counts by position
      --vcf-exclude-non-mutations if set, the output vcf file will exclude positions where no mutations were detected
      --distinguish-query-ends <fraction> (default 0.1) In the output vcf file, we separately display which queries aligned at each position with <fraction> of the end of the query and which didn't.

    Summary by genome

      --out-refs-map-count <file> the output file to summarize the number of reads mapped to each combination of references

    Raw output

      --out-sam <file> the output file in SAM format

      --out-unaligned <file> output file containing unaligned reads. Must have a .fasta or .fastq extension

    --no-output if no output is requested, skip writing output rather than throwing an error

    Debugging

      -v, --verbose output diagnostic information

      --verbose-alignment output verbose information about the alignment process
        This is more than what is output by --verbose

      --verbose-reference output verbose information about the process of analyzing the reference

      -vv implies --verbose-alignment and --verbose-reference

      --verbosity-auto set verbosity flags dynamically and automatically based on the data and alignment

      --out-ancestor <file> file to write our inferred ancestors. See also --no-infer-ancestors

  Multiple output formats may be specified during a single run; for example:

    --out-sam out.sam --out-unaligned out.fastq

  ALIGNMENT PARAMETERS:

    --max-penalty <fraction> (default 0.1) for a match to be reported, its penalty must be no larger than this value times its length
      Setting this closer to 0 will run more quickly

    --max-penalty-span <extraPenalty> (default --snp-penalty / 2) After Mapper finds an alignment having a certain penalty, Mapper will also look for and report alignments having penalties no more than <extraPenalty> additional penalty.
      To only report alignments having the minimum penalty, set this to 0.

    Computing the penalty of a match:

      --new-indel-penalty <penalty> (default 2) the penalty of a new insertion or deletion of length 1

      --extend-indel-penalty <penalty> (default 0.5) the penalty of an extension to an existing insertion or deletion

      --snp-penalty <penalty> (default 1) the penalty of a point mutation

    --max-num-matches <count> (default unlimited) the maximum number of positions on the reference that any query may match.
      Any query that appears to match more than this many positions on the reference will be reported as unmatched.

  OTHER:

    --num-threads <count> number of threads to use at once for processing. Higher values will run more quickly on a system that has that many CPUs available.

## If you're working on a bioinformatics project and would be interested in some consulting help check out our website at https://omicode.info !
