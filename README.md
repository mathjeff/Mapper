# X-Mapper: a fast, accurate aligner for genomic sequences

Download the latest release version here: https://github.com/mathjeff/Mapper/releases/download/1.2.0-beta08/x-mapper-1.2.0-beta08.jar

Read about the algorithm, plus benchmarking and application in the publication here: https://genomebiology.biomedcentral.com/articles/10.1186/s13059-024-03473-7

If you already have aligned sequences and just want to identify genetic variants, see [QuickVariants](https://github.com/caozhichongchong/QuickVariants).

Contact:\
 Dr. Anni Zhang, MIT, anniz44@mit.edu

## Usage:
  java -jar x-mapper.jar [--out-mutations <out.txt>] [--out-vcf <out.vcf>] [--out-sam <out.sam>] [--out-refs-map-count <counts.txt>] [--out-unaligned <unaligned.fastq>] --reference <ref.fasta> --queries <queries.fastq> [options]

    Aligns genomic sequences quickly and accurately using relatively high amounts of memory

  INPUT:

    --reference <file> the reference to use. Should be in .fasta/.fa/.fna or .fastq/.fq format or a .gz of one of those formats.
      May be specified multiple times for multiple reference genomes

    --queries <file> the reads to align to the reference. Should be in .fastq or .fasta format.
      May be specified multiple times for multiple query files
    --paired-queries <file1> <file2> [--spacing <mean> <distancePerPenalty>] Illumina-style paired-end reads to align to the reference. For each read <r1> in <file1> and the corresponding <r2> in <file2>, the alignment position of <r1> is expected to be slightly before the alignment position of the reverse complement of <r2>.
      Each file should be in .fastq/.fasta format
      May be specified multiple times for multiple query files

      --spacing <expected> <distancePerPenalty> (default: 100 50)
        Any query alignment whose inner distance deviates from <expected> has an additional penalty added.
        That additional penalty equals (the difference between the actual distance and <expected>) divided by <distancePerPenalty>, unless the two query sequence alignments would overlap, in which case the additional penalty is 0.

    --infer-ancestors
      Requests that X-Mapper look for parts of the genome that likely shared a common ancestor in the past, and will lower the penalty of an alignment that mismatches the given reference but matches the inferred common ancestor.
    --no-infer-ancestors
      Disables ancestor inference.

    --split-queries-past-size <size> Any queries longer than <size> will be split into smaller queries.
      THIS OPTION IS A TEMPORARY EXPERIMENT FOR LONG READS TO DETECT REARRANGEMENTS AND IMPROVE PERFORMANCE.

    --no-gapmers
      When X-Mapper attempts to identify locations at which the query might align to the reference, X-Mapper first splits the query into smaller pieces and looks for an exact match for each piece.
      By default, these pieces contain noncontiguous basepairs and might look like XXXXXXXX____XXXX.
      This flag makes these pieces be contiguous instead to look more like XXXXXXXXXXXX.
      THIS OPTION IS A TEMPORARY EXPERIMENT FOR TESTING THE PERFORMANCE OF GAPMERS.

    --allow-duplicate-contig-names if multiple contigs have the same name, continue instead of throwing an error.
      This can be confusing but shouldn't cause any incorrect results.

  OUTPUT FORMATS:

    Summary by reference position

      --out-vcf <file> output file to generate containing a description of mutation counts by position
        Details about the file format are included in the top of the file
      --vcf-exclude-non-mutations if set, the output vcf file will exclude positions where no mutations were detected
      --vcf-omit-support-reads By default, the vcf file has a column showing one or more supporting reads for each variant. If set, the output vcf file will hide the supporting reads for each variant.
      --distinguish-query-ends <fraction> (default 0.1) In the output vcf file, we separately display which queries aligned at each position with <fraction> of the end of the query and which didn't.
      --snp-threshold <min total depth> <min supporting depth fraction> (default 0, 0)
        The minimum total depth and minimum supporting depth fraction required at a position to report the support for the mutation

      --indel-start-threshold <min total depth> <min supporting depth fraction> (default 0, 0)
        The minimum total (middle) depth and minimum supporting depth fraction required at a position to report support for the start of an insertion or deletion

      --indel-continue-threshold <min total depth> <min supporting depth fraction> (default 0, 0)
        The minimum total (middle) depth and minimum supporting depth fraction required at a position to report support for a continuation of an insertion or deletion
      --indel-threshold <min total depth> <min supporting depth fraction>
        Alias for --indel-start-threshold <min total depth> <min supporting depth frequency> and --indel-continue-threshold <min total depth> <min supporting depth frequency>


    Summary by mutation

      --out-mutations <file> output file to generate listing the mutations of the queries compared to the reference genome
        Details about the file format are included in the top of the file

      --distinguish-query-ends <fraction> (default 0.1) When detecting indels, only consider the middle <fraction> of each query

      --snp-threshold <min total depth> <min supporting depth fraction> (default 5, 0.9)
        The minimum total depth and minimum supporting depth fraction required at a position to report it as a point mutation

      --indel-start-threshold <min total depth> <min supporting depth fraction> (default 1, 0.8)
        The minimum total (middle) depth and minimum supporting depth fraction required at a position to report it as the start of an insertion or deletion

      --indel-continue-threshold <min total depth> <min supporting depth fraction> (default 1, 0.7)
        The minimum total (middle) depth and minimum supporting depth fraction required at a position to report it as a continuation of an insertion or deletion
      --indel-threshold <min total depth> <min supporting depth fraction>
        Alias for --indel-start-threshold <min total depth> <min supporting depth frequency> and --indel-continue-threshold <min total depth> <min supporting depth frequency>

    Summary by genome

      --out-refs-map-count <file> the output file to summarize the number of reads mapped to each combination of references

    Raw output

      --out-sam <file> the output file in SAM format
        If <file> is '-', the SAM output will be written to stdout instead

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

    --max-penalty-span <extraPenalty> (default --snp-penalty / 2) After X-Mapper finds an alignment having a certain penalty, X-Mapper will also look for and report alignments having penalties no more than <extraPenalty> additional penalty.
      To only report alignments having the minimum penalty, set this to 0.

    Computing the penalty of a match:

      --snp-penalty <penalty> (default 1) the penalty of a point mutation

      --ambiguity-penalty <penalty> (default --max-penalty) the penalty of a fully ambiguous position.
        This penalty is applied in the case of an unknown basepair in the reference ('N').

      --new-indel-penalty <penalty> (default 1.5) the penalty of a new insertion or deletion of length 0

      --extend-indel-penalty <penalty> (default 0.5) the penalty of an extension to an existing insertion or deletion

      --additional-extend-insertion-penalty <penalty> (default <ambiguity penalty>) additional penalty of an extension to an existing insertion

    --max-num-matches <count> (default unlimited) the maximum number of positions on the reference that any query may match.
      Any query that appears to match more than this many positions on the reference will be reported as unmatched.

  OTHER:

    Memory usage: to control the amount of memory that Java makes available to X-Mapper, give the appropriate arguments to Java:

      -Xmx<amount> set <amount> as the maximum amount of memory to use.
      -Xms<amount> set <amount> as the initial amount of memory to use.

      For example, to start with 200 megabytes and increase up to 4 gigabytes as needed, do this

        java -Xms200m -Xmx4g -jar x-mapper.jar <other x-mapper arguments>

    --num-threads <count> number of threads to use at once for processing. Higher values will run more quickly on a system that has that many CPUs available.

    --cache-dir <dir> save and load analyses from this directory to save time.
      Currently what we save here is most of our analyses of the reference genomes (information relating to --infer-ancestors is not currently saved).
      You may specify the same <dir> for multiple executions; data is actually stored in an appropriate subdirectory.

To try an example, see examples/test.sh

To make changes to X-Mapper, see [DEVELOPING.md](DEVELOPING.md)

## If you're working on a bioinformatics project and would be interested in some consulting help, check out our website at https://genomiverse.net/ !
