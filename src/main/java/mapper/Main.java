package mapper;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import javax.management.MBeanServer;

public class Main {

  static Logger alignmentLogger;
  static Logger referenceLogger;
  static Logger silentLogger = Logger.NoOpLogger;

  static int defaultExpectedDistanceBetweenPairedSequences = 100;
  static int defaultSpacingDeviationPerUnitPenalty = 50;

  public static void main(String[] args) throws IllegalArgumentException, FileNotFoundException, IOException, InterruptedException {
    long startMillis = System.currentTimeMillis();
    MapperMetadata.setMainArguments(args);

    //System.out.println(MapperMetadata.guessCommandLine());
    System.out.println("Mapper version " + MapperMetadata.getVersion());

    // parse arguments
    List<String> referencePaths = new ArrayList<String>();
    List<QueryProvider> queries = new ArrayList<QueryProvider>();
    String outVcfPath = null;
    String outSamPath = null;
    String outUnalignedPath = null;
    String outAncestorPath = null;
    boolean enableGapmers = true;
    boolean vcfIncludeNonMutations = true;
    String outRefsMapCountPath = null;
    int alignmentVerbosity = 0;
    int referenceVerbosity = 0;
    boolean allowNoOutput = false;
    boolean autoVerbose = false;
    boolean guessReferenceAncestors = false;

    double mutationPenalty = -1; // default filled in later
    double indelStart_penalty = 1.5;
    double indelExtension_penalty = 0.5;
    double additional_insertionExtension_penalty = -1; // default filled in later
    double maxErrorRate = -1; // default filled in later
    double ambiguityPenalty = -1;
    int maxNumMatches = Integer.MAX_VALUE;
    double max_penaltySpan = -1;

    int numThreads = 1;
    double queryEndFraction = 0.1;
    int splitQueriesPastSize = -1;

    boolean hasPairedQueriesWithoutSpecifyingSpacing = false;

    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if ("--reference".equals(arg)) {
        referencePaths.add(args[i + 1]);
        i++;
        continue;
      }
      if ("--queries".equals(arg)) {
        String queryPath = args[i + 1];
        SequenceProvider sequenceProvider = DataLoader.LoadFrom(queryPath, true);
        if (splitQueriesPastSize > 0) {
          sequenceProvider = new SequenceSplitter(splitQueriesPastSize, sequenceProvider);
        }
        QueryProvider queryBuilder = new SimpleQueryProvider(sequenceProvider);
        queries.add(queryBuilder);
        i++;
        continue;
      }
      if ("--paired-queries".equals(arg)) {
        if (splitQueriesPastSize > 0) {
          throw new IllegalArgumentException("Sorry, " + arg + " is not currently supported with --split-queries-past-size");
        }
        String leftsPath = args[i + 1];
        SequenceProvider lefts = DataLoader.LoadFrom(leftsPath, true);
        String rightsPath = args[i + 2];
        SequenceProvider rights = DataLoader.LoadFrom(rightsPath, true);
        i += 2;

        double expectedDistanceBetweenPairedSequences = defaultExpectedDistanceBetweenPairedSequences;
        double spacingDeviationPerUnitPenalty = defaultSpacingDeviationPerUnitPenalty;
        if (i + 1 < args.length && "--spacing".equals(args[i + 1])) {
          i++;
          expectedDistanceBetweenPairedSequences = Double.parseDouble(args[i + 1]);
          i++;
          spacingDeviationPerUnitPenalty = Double.parseDouble(args[i + 1]);
          i++;
        } else {
          hasPairedQueriesWithoutSpecifyingSpacing = true;
        }

        QueryProvider queryBuilder = new PairedEndQueryProvider(lefts, rights, expectedDistanceBetweenPairedSequences, spacingDeviationPerUnitPenalty);
        queries.add(queryBuilder);
        continue;
      }
      if ("--split-queries-past-size".equals(arg)) {
        if (queries.size() > 0) {
          throw new IllegalArgumentException("Sorry, " + arg + " currently is only supported before --queries");
        }
        splitQueriesPastSize = Integer.parseInt(args[i + 1]);
        i++;
        continue;
      }
      if ("--out-vcf".equals(arg)) {
        outVcfPath = args[i + 1];
        i++;
        continue;
      }
      if ("--out-sam".equals(arg)) {
        outSamPath = args[i + 1];
        i++;
        continue;
      }
      if ("--out-unaligned".equals(arg)) {
        outUnalignedPath = args[i + 1];
        i++;
        continue;
      }
      if ("--out-refs-map-count".equals(arg)) {
        outRefsMapCountPath = args[i + 1];
        i++;
        continue;
      }
      if ("--out-ancestor".equals(arg)) {
        outAncestorPath = args[i + 1];
        i++;
        continue;
      }
      if ("--no-gapmers".equals(arg)) {
        enableGapmers = false;
        continue;
      }
      if ("--no-output".equals(arg)) {
        allowNoOutput = true;
        continue;
      }
      if ("--verbose".equals(arg) || "-v".equals(arg)) {
        alignmentVerbosity = Math.max(alignmentVerbosity, 1);
        continue;
      }
      if ("--verbose-alignment".equals(arg)) {
        alignmentVerbosity = Math.max(alignmentVerbosity, Integer.MAX_VALUE);
        continue;
      }
      if ("--verbose-reference".equals(arg)) {
        referenceVerbosity = Math.max(referenceVerbosity, 1);
        continue;
      }
      if ("-vv".equals(arg)) {
        alignmentVerbosity = Math.max(alignmentVerbosity, Integer.MAX_VALUE);
        referenceVerbosity = Math.max(referenceVerbosity, 1);
        continue;
      }
      if ("--verbosity-auto".equals(arg)) {
        autoVerbose = true;
        continue;
      }
      if ("--new-indel-penalty".equals(arg)) {
        String value = args[i + 1];
        indelStart_penalty = Double.parseDouble(value);
        i++;
        continue;
      }
      if ("--extend-indel-penalty".equals(arg)) {
        String value = args[i + 1];
        indelExtension_penalty = Double.parseDouble(value);
        i++;
        continue;
      }
      if ("--additional-extend-insertion-penalty".equals(arg)) {
        String value = args[i + 1];
        additional_insertionExtension_penalty = Double.parseDouble(value);
        i++;
        continue;
      }
      if ("--snp-penalty".equals(arg)) {
        String value = args[i + 1];
        mutationPenalty = Double.parseDouble(value);
        if (mutationPenalty <= 0) {
          usageError("--snp-penalty must be > 0");
        }
        i++;
        continue;
      }
      if ("--max-penalty".equals(arg)) {
        String value = args[i + 1];
        maxErrorRate = Double.parseDouble(value);
        if (maxErrorRate < 0) {
          usageError("--max-penalty must be >= 0");
        }
        i++;
        continue;
      }
      if ("--max-penalty-span".equals(arg)) {
        String value = args[i + 1];
        max_penaltySpan = Double.parseDouble(value);
        if (max_penaltySpan < 0) {
          usageError("--max-penalty-span must be >= 0");
        }
        i++;
        continue;
      }
      if ("--ambiguity-penalty".equals(arg)) {
        String value = args[i + 1];
        ambiguityPenalty = Double.parseDouble(value);
        if (ambiguityPenalty < 0) {
          usageError("--ambiguity-penalty must be >= 0");
        }
        i++;
        continue;
      }
      if ("--max-num-matches".equals(arg)) {
        String value = args[i + 1];
        maxNumMatches = Integer.parseInt(value);
        i++;
        continue;
      }
      if ("--num-threads".equals(arg)) {
        String value = args[i + 1];
        numThreads = Integer.parseInt(value);
        i++;
        continue;
      }
      if ("--distinguish-query-ends".equals(arg)) {
        String value = args[i + 1];
        queryEndFraction = Double.parseDouble(value);
        i++;
        continue;
      }
      if ("--vcf-exclude-non-mutations".equals(arg)) {
        vcfIncludeNonMutations = false;
        continue;
      }
      if ("--infer-ancestors".equals(arg)) {
        guessReferenceAncestors = true;
        continue;
      }
      if ("--no-infer-ancestors".equals(arg)) {
        guessReferenceAncestors = false;
        continue;
      }
      if ("--spacing".equals(arg)) {
        usageError("--spacing is not a top-level argument: try --paired-queries <queries> <queries2> --spacing <expected> <distancePerPenalty>");
      }
      if (arg.startsWith("-Xmx") || arg.startsWith("-Xms")) {
        usageError("" + arg + " is not a Mapper argument: try `java " + arg + " -jar <arguments>`");
      }
      usageError("Unrecognized argument: " + arg);
    }
    if (referencePaths.size() < 1) {
      usageError("--reference is required");
    }
    if (queries.size() < 1) {
      usageError("--queries or --paired-queries is required");
    }
    if (outVcfPath == null && outSamPath == null && outRefsMapCountPath == null && outUnalignedPath == null && !allowNoOutput) {
      usageError("No output specified. Try --out-vcf <output path>, or if you really don't want to generate an output file, --no-output");
    }
    alignmentLogger = new Logger(new PrintWriter(), 1, alignmentVerbosity);
    referenceLogger = new Logger(new PrintWriter(), 1, referenceVerbosity);
    if (maxErrorRate >= 0 && mutationPenalty >= 0 && hasPairedQueriesWithoutSpecifyingSpacing) {
      usageError("Customized alignment penalties (--snp-penalty) and penalty threshold (--max-error-rate) without customizing spacing penalty between paired-end queries (--paired-queries <queries> <queries2> --spacing <expected> <distancePerPenalty>).\n Please specify --spacing explicitly.\n If you really don't want to change the default spacing penalty, you can specify --spacing " + defaultExpectedDistanceBetweenPairedSequences + " " + defaultSpacingDeviationPerUnitPenalty);
    }

    if (maxErrorRate < 0) {
      maxErrorRate = 0.1;
    }
    if (mutationPenalty <= 0) {
      mutationPenalty = 1;
    }
    if (indelExtension_penalty <= 0) {
      usageError("--extend-indel-penalty must be > 0");
    }
    if (indelStart_penalty <= 0) {
      usageError("--new-indel-penalty must be > 0");
    }
    if (maxNumMatches < 1) {
      usageError("--max-num-matches must be >= 1");
    }
    if (numThreads < 1) {
      usageError("--num-threads must be >= 1");
    }
    if (queryEndFraction < 0 || queryEndFraction >= 1) {
      usageError("--distinguish-query-ends must be >= 0 and < 1");
    }

    if (max_penaltySpan < 0) {
      max_penaltySpan = mutationPenalty / 2;
    }
    // If an alignment contains a fully ambiguous position, that position doesn't increase or decrease our confidence in the alignment
    // So, by default, the penalty for the ambiguous position cancels out the increase in penalty threshold from having one extra position
    if (ambiguityPenalty < 0) {
      ambiguityPenalty = maxErrorRate;
    }

    // choose default penalty for extending an insertion
    if (additional_insertionExtension_penalty < 0) {
      // extending an insertion by some number of base pairs reduces the number of base pairs matching in the query by the same number of base pairs
      additional_insertionExtension_penalty = ambiguityPenalty;
    }

    AlignmentParameters parameters = new AlignmentParameters();
    parameters.MutationPenalty = mutationPenalty;
    parameters.DeletionStart_Penalty = indelStart_penalty;
    parameters.DeletionExtension_Penalty = indelExtension_penalty;
    parameters.InsertionStart_Penalty = parameters.DeletionStart_Penalty;
    parameters.InsertionExtension_Penalty = parameters.DeletionExtension_Penalty + additional_insertionExtension_penalty;
    parameters.MaxErrorRate = maxErrorRate;
    parameters.UnalignedPenalty = maxErrorRate;
    parameters.AmbiguityPenalty = ambiguityPenalty;
    parameters.MaxNumMatches = maxNumMatches;
    parameters.Max_PenaltySpan = max_penaltySpan;

    System.out.println("" + referencePaths.size() + " reference files:");
    for (String referencePath: referencePaths) {
      System.out.println("Reference path = " + referencePath);
    }
    System.out.println("" + queries.size() + " sets of queries: ");
    for (QueryProvider queryBuilder : queries) {
      System.out.println(queryBuilder.toString());
    }
    boolean successful = run(referencePaths, queries, outVcfPath, vcfIncludeNonMutations, outSamPath, outRefsMapCountPath, outUnalignedPath, parameters, numThreads, queryEndFraction, autoVerbose, guessReferenceAncestors, outAncestorPath, enableGapmers, startMillis);
    if (successful)
      System.exit(0);
    else
      System.exit(1);
  }

  public static void usageError(String message) {
    System.err.println(
"\n" +
"Usage:\n"+
"  java -jar mapper.jar [--out-vcf <out.vcf>] [--out-sam <out.sam>] [--out-refs-map-count <counts.txt>] [--out-unaligned <unaligned.fastq>] --reference <ref.fasta> --queries <queries.fastq> [options]\n" +
"  java -jar mapper.jar [--out-vcf <out.vcf>] [--out-sam <out.sam>] [--out-refs-map-count <counts.txt>] [--out-unaligned <unaligned.fastq>] --reference <ref.fasta> --paired-queries [--spacing <expected> <distancePerPenalty>]<queries.fastq> <queries2.fastq> [options]\n" +
"\n" +
"    Aligns genomic sequences quickly\n" +
"\n" +
"  INPUT:\n" +
"\n" +
"    --reference <file> the reference to use. Should be in .fasta/.fa/.fna or .fastq/.fq format or a .gz of one of those formats.\n" +
"\n" +
"    --queries <file> the reads to align to the reference. Should be in .fastq or .fasta format.\n" +
"      May be specified multiple times for multiple query files\n" +
"    --paired-queries <file1> <file2> [--spacing <mean> <distancePerPenalty>] Illumina-style paired-end reads to align to the reference. For each read <r1> in <file1> and the corresponding <r2> in <file2>, the alignment position of <r1> is expected to be slightly before the alignment position of the reverse complement of <r2>.\n" +
"      Each file should be in .fastq/.fasta format\n" +
"      May be specified multiple times for multiple query files\n" +
"\n" +
"      --spacing <expected> <distancePerPenalty> (default: " + defaultExpectedDistanceBetweenPairedSequences + " " + defaultSpacingDeviationPerUnitPenalty + ")\n" +
"        Any query alignment whose inner distance deviates from <expected> has an additional penalty added.\n"+
"        That additional penalty equals (the difference between the actual distance and <expected>) divided by <distancePerPenalty>, unless the two query sequence alignments would overlap, in which case the additional penalty is 0.\n" +
"\n" +
"    --infer-ancestors\n" +
"      Requests that Mapper look for parts of the genome that likely shared a common ancestor in the past, and will lower the penalty of an alignment that mismatches the given reference but matches the inferred common ancestor.\n" +
"    --no-infer-ancestors\n" +
"      Disables ancestor inference.\n" +
"\n" +
"    --split-queries-past-size <size> Any queries longer than <size> will be split into smaller queries.\n" +
"      THIS OPTION IS A TEMPORARY EXPERIMENT FOR DETECTING REARRANGEMENTS IN LONG READS.\n" +
"\n" +
"    --no-gapmers\n" +
"      When Mapper attempts to identify locations at which the query might align to the reference, Mapper first splits the query into smaller pieces and looks for an exact match for each piece.\n" +
"      By default, these pieces contain noncontiguous basepairs and might look like XXXXXXXX____XXXX.\n" +
"      This flag makes these pieces be contiguous instead to look more like XXXXXXXXXXXX.\n" +
"      THIS OPTION IS A TEMPORARY EXPERIMENT FOR TESTING THE PERFORMANCE OF GAPMERS.\n" +
"\n" +
"  OUTPUT FORMATS:\n" +
"\n" +
"    Summary by position\n" +
"\n" +
"      --out-vcf <file> output file to generate containing a description of mutation counts by position\n" +
"      --vcf-exclude-non-mutations if set, the output vcf file will exclude positions where no mutations were detected\n" +
"      --distinguish-query-ends <fraction> (default 0.1) In the output vcf file, we separately display which queries aligned at each position with <fraction> of the end of the query and which didn't.\n" +
"\n" +
"    Summary by genome\n" +
"\n" +
"      --out-refs-map-count <file> the output file to summarize the number of reads mapped to each combination of references\n" +
"\n" +
"    Raw output\n" +
"\n" +
"      --out-sam <file> the output file in SAM format\n" +
"\n" +
"      --out-unaligned <file> output file containing unaligned reads. Must have a .fasta or .fastq extension\n" +
"\n" +
"    --no-output if no output is requested, skip writing output rather than throwing an error\n" +
"\n" +
"    Debugging\n" +
"\n" +
"      -v, --verbose output diagnostic information\n" +
"\n" +
"      --verbose-alignment output verbose information about the alignment process\n" +
"        This is more than what is output by --verbose\n" +
"\n" +
"      --verbose-reference output verbose information about the process of analyzing the reference\n" +
"\n" +
"      -vv implies --verbose-alignment and --verbose-reference\n" +
"\n" +
"      --verbosity-auto set verbosity flags dynamically and automatically based on the data and alignment\n" +
"\n" +
"      --out-ancestor <file> file to write our inferred ancestors. See also --no-infer-ancestors\n" +
"\n" +
"  Multiple output formats may be specified during a single run; for example:\n" +
"\n" +
"    --out-sam out.sam --out-unaligned out.fastq\n" +
"\n" +
"  ALIGNMENT PARAMETERS:\n" +
"\n" +
"    --max-penalty <fraction> (default 0.1) for a match to be reported, its penalty must be no larger than this value times its length\n" +
"      Setting this closer to 0 will run more quickly\n" +
"\n" +
"    --max-penalty-span <extraPenalty> (default --snp-penalty / 2) After Mapper finds an alignment having a certain penalty, Mapper will also look for and report alignments having penalties no more than <extraPenalty> additional penalty.\n" +
"      To only report alignments having the minimum penalty, set this to 0.\n" +
"\n" +
"    Computing the penalty of a match:\n" +
"\n" +
"      --snp-penalty <penalty> (default 1) the penalty of a point mutation\n" +
"\n" +
"      --ambiguity-penalty <penalty> (default --max-penalty) the penalty of a fully ambiguous position.\n" +
"        This penalty is applied in the case of an unknown basepair in the reference ('N').\n" +
"\n" +
"      --new-indel-penalty <penalty> (default 1.5) the penalty of a new insertion or deletion of length 0\n" +
"\n" +
"      --extend-indel-penalty <penalty> (default 0.5) the penalty of an extension to an existing insertion or deletion\n" +
"\n" +
"      --additional-extend-insertion-penalty <penalty> (default <ambiguity penalty>) additional penalty of an extension to an existing insertion\n" +
"\n" +
"    --max-num-matches <count> (default unlimited) the maximum number of positions on the reference that any query may match.\n" +
"      Any query that appears to match more than this many positions on the reference will be reported as unmatched.\n" +
"\n" +
"  OTHER:\n" +
"\n" +
"    Memory usage: to control the amount of memory that Java makes available to Mapper, give the appropriate arguments to Java:\n" +
"\n" +
"      -Xmx<amount> set <amount> as the maximum amount of memory to use.\n" +
"      -Xms<amount> set <amount> as the initial amount of memory to use.\n" +
"\n" +
"      For example, to start with 200 megabytes and increase up to 4 gigabytes as needed, do this\n" +
"\n" +
"        java -Xms200m -Xmx4g -jar mapper.jar <other mapper arguments>\n" +
"\n" +
"    --num-threads <count> number of threads to use at once for processing. Higher values will run more quickly on a system that has that many CPUs available.\n"
);

    System.err.println(message);
    System.exit(1);
  }

  // performs alignment and outputs results
  public static boolean run(List<String> referencePaths, List<QueryProvider> queriesList, String outVcfPath, boolean vcfIncludeNonMutations, String outSamPath, String outRefsMapCountPath, String outUnalignedPath, AlignmentParameters parameters, int numThreads, double queryEndFraction, boolean autoVerbose, boolean guessReferenceAncestors, String outAncestorPath, boolean enableGapmers, long startMillis) throws IllegalArgumentException, FileNotFoundException, IOException, InterruptedException {
    VcfWriter writer = null;
    if (outVcfPath != null)
      writer = new VcfWriter(outVcfPath, vcfIncludeNonMutations, queryEndFraction);

    System.out.println("Loading reference");
    boolean keepQualityData = (outUnalignedPath != null);
    SequenceProvider reference = DataLoader.LoadFrom(referencePaths, false);
    List<Sequence> sortedReference = sortAndComplementReference(reference);
    ReferenceProvider referenceProvider;
    SequenceDatabase originalReference = new SequenceDatabase(sortedReference);

    int minDuplicationLength = DuplicationDetector.chooseMinDuplicationLength(originalReference);
    int maxDuplicationLength = DuplicationDetector.chooseMaxDuplicationLength(originalReference);
    // The main purpose of the DuplicationDetector is to detect regions of the genome that have more duplication than normal, so we increase the maximum number of short matches that its HashBlock_Database is willing to provide
    // This shouldn't be very slow because we don't run the DuplicationDetector very many times, unlike aligning queries
    int duplicationDetector_maxNumShortMatches = 8;
    StatusLogger statusLogger = new StatusLogger(new Logger(new PrintWriter()), startMillis);

    if (guessReferenceAncestors) {
      HashBlock_Database originalReference_database = new HashBlock_Database(originalReference, minDuplicationLength, maxDuplicationLength, duplicationDetector_maxNumShortMatches, enableGapmers, statusLogger);
      DuplicationDetector ancestryDuplicationDetector = new DuplicationDetector(originalReference_database, minDuplicationLength, maxDuplicationLength, 3, 1, statusLogger);
      double dissimilarityThreshold = parameters.MaxErrorRate / parameters.MutationPenalty;
      referenceProvider = new AncestryDetector(ancestryDuplicationDetector, sortedReference, dissimilarityThreshold, statusLogger).setOutputPath(outAncestorPath).setResultingDatabaseEnableGapmers(enableGapmers);

    } else {
      referenceProvider = new HashBlock_Database(originalReference, -1, maxDuplicationLength, -1, enableGapmers, statusLogger);
    }

    DuplicationDetector approximateDuplicationDetector = new DuplicationDetector(referenceProvider, minDuplicationLength, maxDuplicationLength, 2, 100, statusLogger);

    long now = System.currentTimeMillis();
    long elapsed = (now - startMillis) / 1000;

    QueryProvider queries = new QueriesIterator(queriesList);

    List<AlignmentListener> listeners = new ArrayList<AlignmentListener>();
    MatchDatabase matchDatabase = new MatchDatabase(queryEndFraction);
    ReferenceAlignmentCounter referenceAlignmentCounter = new ReferenceAlignmentCounter();
    AlignmentCounter matchCounter = new AlignmentCounter();
    if (outVcfPath != null) {
      listeners.add(matchDatabase);
    }
    SamWriter samWriter = null;
    if (outSamPath != null) {
      samWriter = new SamWriter(originalReference, outSamPath, queries.get_containsPairedEndReads());
      listeners.add(samWriter);
    }
    UnalignedQuery_Writer unalignedWriter = null;
    if (outUnalignedPath != null) {
      unalignedWriter = new UnalignedQuery_Writer(outUnalignedPath, queries.get_allReadsContainQualityInformation());
      listeners.add(unalignedWriter);
    }
    if (outRefsMapCountPath != null) {
      listeners.add(referenceAlignmentCounter);
    }
    listeners.add(matchCounter);
    AlignmentCache alignmentCache = new AlignmentCache();
    AlignmentStatistics statistics = compare(referenceProvider, queries, approximateDuplicationDetector, startMillis, parameters, numThreads, queryEndFraction, alignmentCache, listeners, autoVerbose);

    long numMatchingQuerySequences = matchCounter.getNumMatchingSequences();
    long numQuerySequences = matchCounter.getNumSequences();
    long matchPercent;
    if (numQuerySequences > 0)
      matchPercent = numMatchingQuerySequences * 100 / numQuerySequences;
    else
      matchPercent = 0;
    long totalAlignedQueryLength = matchCounter.getTotalAlignedQueryLength();
    double totalAlignedPenalty = matchCounter.getTotalAlignedPenalty();
    float averagePenaltyPerBase = (float)(totalAlignedPenalty / totalAlignedQueryLength); // round

    // output referenceAlignmentCounter RefsMapCount
    if (outRefsMapCountPath != null) {
      long computationEnd = System.currentTimeMillis();
      long computationTime = (computationEnd - startMillis) / 1000;
      System.out.println("Writing RefsMapCount results at " + computationTime + "s");
      referenceAlignmentCounter.sumAlignments(outRefsMapCountPath);
      long writingEnd = System.currentTimeMillis();
      long writingTime = (writingEnd - startMillis) / 1000;
      System.out.println("Saved " + outRefsMapCountPath + " at " + writingTime + "s");
    }
    String displayCoverage = null;
    if (outVcfPath != null) {
      SequenceDatabase referenceDatabase = referenceProvider.get_HashBlock_database(referenceLogger).getSequenceDatabase();
      Map<Sequence, Alignments> alignments = matchDatabase.groupByPosition();
      long computationEnd = System.currentTimeMillis();
      double computationTime = (double)(computationEnd - startMillis) / 1000.0;
      System.out.println("Writing vcf results at " + computationTime + "s");
      writer.write(alignments, numThreads);
      System.out.println("Saved " + outVcfPath);
      long numMatchedPositions = writer.getNumReferencePositionsMatched();
      long numPositions = referenceDatabase.getTotalForwardSize();

      // Format the coverage as a human-readable string
      double coverage = ((double)numMatchedPositions) / ((double)numPositions);
      displayCoverage = "" + (int)(coverage * 100) + "%";
      // If the coverage is less than 1% but more than 0%, emphasize that to make it easy to notice
      if (displayCoverage.equals("0%") && coverage > 0) {
        displayCoverage = "<1%";
      }
      displayCoverage = " Coverage                      : " + displayCoverage + " of the reference (" + numMatchedPositions + "/" + numPositions + ") was matched";
    }
    // show statistics
    System.out.println("");
    System.out.println("Statistics: ");
    if (matchCounter.getNumMatchingSequences() != matchCounter.getNumAlignedQueries()) {
      // paired-end reads
      Distribution distance = matchCounter.getDistanceBetweenQueryComponents();
      System.out.println(" Query pair separation distance: avg: " + (float)distance.getMean() + " stddev: " + (float)distance.getStdDev() + " (adjust via --spacing)");
    }
    System.out.println(" Alignment rate                : " + matchPercent + "% of query sequences (" + numMatchingQuerySequences + "/" + numQuerySequences + ")");
    if (displayCoverage != null) {
      System.out.println(displayCoverage);
    }
    System.out.println(" Average penalty               : " + averagePenaltyPerBase + " per base (" + (long)totalAlignedPenalty + "/" + (long)totalAlignedQueryLength + ") in aligned queries");
    if (statistics != null) {
      long numIndels = statistics.numIndels;
      float indelsPerPosition = (float)numIndels / (float)totalAlignedQueryLength;
      System.out.println(" Num indels                    : " + indelsPerPosition + " per base (" + numIndels + "/" + (long)totalAlignedQueryLength + ") in aligned queries");
    }

    if (statistics != null) {
      System.out.println("");
      System.out.println("Performance:");

      System.gc();
      Runtime runtime = Runtime.getRuntime();
      long maxAllowedMemory = runtime.maxMemory();
      long usedMemory = runtime.totalMemory() - runtime.freeMemory();
      long usedMemoryMB = usedMemory / 1024 / 1024;
      System.out.println(" Ending memory usage: " + usedMemoryMB + "mb");

      Query slowestQuery = statistics.slowestQuery;

      if (slowestQuery != null) {
        String queryDisplayText = slowestQuery.format();
        String numAlignmentsText;
        int numAlignments = statistics.slowestQueryNumAlignments;
        if (numAlignments == 1)
          numAlignmentsText = "1 time";
        else
          numAlignmentsText = "" + numAlignments + " times";
        System.out.println(" Slowest query: #" + slowestQuery.getId() + " (" + statistics.slowestQueryMillis + "ms) : " + queryDisplayText + " aligned " + numAlignmentsText);
      }

      Query queryAtRandomMoment = statistics.queryAtRandomMoment;
      if (queryAtRandomMoment != null) {
        System.out.println(" Query at random moment: #" + queryAtRandomMoment.getId() + " : " + queryAtRandomMoment.format());
      }

      int millisOnUnalignedQueries = (int)(statistics.cpuMillisSpentOnUnalignedQueries / 1000 / numThreads);
      System.out.println(" Unaligned queries took        : " + statistics.cpuMillisSpentOnUnalignedQueries + " cpu-ms (" + millisOnUnalignedQueries + "s)");

      int immediateAcceptancePercent = (int)(statistics.numCasesImmediatelyAcceptingFirstAlignment * 100 / statistics.numQueriesLoaded);
      System.out.println(" Immediately accepted          : " + immediateAcceptancePercent + "% alignments (" + statistics.numCasesImmediatelyAcceptingFirstAlignment + "/" + statistics.numQueriesLoaded + ")");
      int millisAligningMatches = (int)(statistics.cpuMillisSpentAligningMatches / 1000 / numThreads);
      System.out.println(" Time aligning matches         : " + statistics.cpuMillisSpentAligningMatches + " cpu-ms (" + millisAligningMatches + "s)");
      int millisThroughOptimisticBestAlignments = (int)(statistics.cpuMillisThroughOptimisticBestAlignments / 1000 / numThreads);
      System.out.println(" Finding optimistic alignments : " + statistics.cpuMillisThroughOptimisticBestAlignments + " cpu-ms (" + millisThroughOptimisticBestAlignments + "s)");
      int queriesLoadedFromCachePercent = (int)((long)100 * (long)statistics.numCacheHits / (long)statistics.numQueriesLoaded);
      int queriesSavedToCachePercent = (int)((long)100 * alignmentCache.getUsage() / (long)statistics.numQueriesLoaded);
      long numQueriesNotInCache = statistics.numQueriesLoaded - alignmentCache.getUsage() - statistics.numCacheHits;
      int queriesNotInCachePercent = (int)((long) 100 * (long)numQueriesNotInCache / (long)statistics.numQueriesLoaded);

      System.out.println(" Alignment cache usage         : " + queriesLoadedFromCachePercent + "% (" + statistics.numCacheHits + ") loaded, " + queriesSavedToCachePercent + "% (" + alignmentCache.getUsage() + ") stored, " + queriesNotInCachePercent + "% (" + numQueriesNotInCache + ") skipped");
      System.out.println(" Time reading queries          : " + statistics.millisReadingQueries + "ms");
      System.out.println(" Time launching workers        : " + statistics.millisLaunchingWorkers + "ms");
      System.out.println(" Time waiting for workers      : " + statistics.millisWaitingForWorkers + "ms");
    }
    String successStatus;
    boolean successful = (statistics != null);
    if (successful)
      successStatus = "Done";
    else
      successStatus = "Failed";
    if (samWriter != null)
      samWriter.close();
    if (unalignedWriter != null)
      unalignedWriter.close();
    long end = System.currentTimeMillis();
    double totalTime = ((double)end - (double)startMillis) / 1000.0;
    System.out.println("");
    System.out.println(successStatus + " in " + totalTime + "s.");
    return successful;
  }

  public static void dumpHeap() throws IOException {
    String outputPath = "mapper.hprof";
    System.out.println("dumping heap to " + outputPath);
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    HotSpotDiagnosticMXBean bean =
        ManagementFactory.newPlatformMXBeanProxy(server,
        "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);
    bean.dumpHeap("mapper.hprof", false);
    System.out.println("dumped heap to " + outputPath);
  }

  public static AlignmentStatistics compare(ReferenceProvider referenceProvider, QueryProvider queries, DuplicationDetector approximateDuplicationDetector, long startMillis, AlignmentParameters parameters, int numThreads, double queryEndFraction, AlignmentCache alignmentCache, List<AlignmentListener> alignmentListeners, boolean autoVerbose) throws InterruptedException, IOException {
    long readingMillis = 0;
    long launchingMillis = 0;
    long waitingMillis = 0;
    int numQueriesForNextMessage = 1;
    long previousElapsedSeconds = 0;

    // Create some workers and assign some queries to each
    Set<AlignerWorker> workers = new HashSet<AlignerWorker>(numThreads);

    long numQueriesLoaded = 0;
    long numQueriesAssigned = 0;
    int maxNumBasesPerJob = 500000;
    int workerIndex = 0;
    boolean doneReadingQueries = false;
    List<List<QueryBuilder>> pendingQueries = new ArrayList<List<QueryBuilder>>();
    long lastPrintTime = 0;
    long nextCountToPrint = 0;
    long slowestAlignmentMillis = -1;
    Query slowestQuery = null;
    QueryAlignments slowestAlignment = null;
    RandomMomentSelector randomMomentSelector = new RandomMomentSelector();
    Query queryAtRandomMoment = null;
    long cpuMillisSpentOnUnalignedQueries = 0;
    long cpuMillisSpentAligningMatches = 0;
    long cpuMillisThroughOptimisticBestAlignments = 0;
    int numCacheHits = 0;
    int numCasesImmediatelyAcceptingFirstAlignment = 0;
    long numIndels = 0;
    BlockingQueue<AlignerWorker> completedWorkers = new ArrayBlockingQueue<AlignerWorker>(numThreads);
    boolean everSaturatedWorkers = false;
    List<AlignerWorker> pendingWorkers = new ArrayList<AlignerWorker>();
    int targetNumPendingJobsPerWorker = 10;
    while (workers.size() > 0 || !doneReadingQueries || pendingQueries.size() > 0) {
      boolean progressed = false;
      if (workers.size() >= numThreads)
        everSaturatedWorkers = true;
      int targetNumPendingJobs = numThreads * targetNumPendingJobsPerWorker;
      // Do we have to read more queries?
      if (!doneReadingQueries && (pendingQueries.size() < 1 || (workers.size() >= numThreads && pendingQueries.size() < targetNumPendingJobs))) {
        long readStart = System.currentTimeMillis();
        // If most workers are idle, then give each worker just a few queries so they can more quickly have something to do
        int targetNumBases = Math.min(maxNumBasesPerJob, Math.max(maxNumBasesPerJob * (pendingQueries.size() + 1) / numThreads, 1));
        // If we haven't yet spawned all of the initial workers, then just give one query to each worker so that this worker will help to hash the reference
        if (!everSaturatedWorkers)
          targetNumBases = 1;
        List<QueryBuilder> batch = new ArrayList<QueryBuilder>();
        int totalLengthOfPendingQueries = 0;
        while (true) {
          if (totalLengthOfPendingQueries >= targetNumBases) {
            break;
          }
          QueryBuilder queryBuilder = queries.getNextQueryBuilder();
          if (queryBuilder == null) {
            doneReadingQueries = true;
            break;
          }
          numQueriesLoaded++;
          queryBuilder.setId(numQueriesLoaded);
          batch.add(queryBuilder);
          totalLengthOfPendingQueries += queryBuilder.getLength();
        }
        pendingQueries.add(batch);
        progressed = true;
        long readEnd = System.currentTimeMillis();
        readingMillis += (readEnd - readStart);
      }

      // if don't have enough active workers, assign a job to a worker
      if (workers.size() < numThreads) {
        long launchStart = System.currentTimeMillis();

        List<QueryBuilder> queriesToProcess;
        // we have enough idle threads to spawn another worker
        if (pendingQueries.size() > 0) {
          // we have queries that haven't been assigned
          queriesToProcess = pendingQueries.get(pendingQueries.size() - 1);
          pendingQueries.remove(pendingQueries.size() - 1);
        } else {
          // each query has been given to some worker
          // We might be able to help hash the reference more quickly, though
          if (referenceProvider.getCanUseHelp()) {
            queriesToProcess = new ArrayList<QueryBuilder>();
          } else {
            queriesToProcess = null;
          }
        }
        if (queriesToProcess != null) {
          long now = System.currentTimeMillis();
          long elapsed = (now - startMillis) / 1000;

          // give these queries to this worker
          Logger workerAlignmentLogger = alignmentLogger.withWriter(new BufferedWriter());
          Logger workerReferenceLogger = referenceLogger.withWriter(new BufferedWriter());
          if (autoVerbose && workerIndex == 0) {
            workerAlignmentLogger = new Logger(new BufferedWriter(), 1, Integer.MAX_VALUE);
          }
          AlignerWorker worker;
          boolean workerAlreadyRunning;
          if (pendingWorkers.size() > 0) {
            worker = pendingWorkers.remove(pendingWorkers.size() - 1);
            workerAlreadyRunning = true;
          } else {
            worker = new AlignerWorker(referenceProvider, parameters, approximateDuplicationDetector.getView(workerReferenceLogger), workerIndex, alignmentListeners, alignmentCache, completedWorkers);
            workerAlreadyRunning = false;
          }
          long estimatedTotalNumQueries;
          if (!doneReadingQueries)
            estimatedTotalNumQueries = numQueriesLoaded * 2;
          else
            estimatedTotalNumQueries = numQueriesLoaded;
          workers.add(worker);
          worker.requestProcess(queriesToProcess, startMillis, estimatedTotalNumQueries, workerAlignmentLogger, workerReferenceLogger);
          numQueriesAssigned += queriesToProcess.size();
          workerIndex++;
          progressed = true;
          if (!workerAlreadyRunning)
            worker.start();

          // determine if enough queries have completed and enough time has passed for it to be worth issuing a status update
          if (numQueriesAssigned >= nextCountToPrint) {
            nextCountToPrint = determineNextCountToReport(numQueriesAssigned);
            if (elapsed != lastPrintTime) {
              // note elapsed != 0 because lastPrintTime starts at 0
              long queriesPerSecond = numQueriesAssigned / elapsed;
              System.out.println("Processing query " + numQueriesAssigned + " at " + elapsed + "s (" + queriesPerSecond + " q/s), " + workers.size() + " workers, " + pendingQueries.size() + "/" + targetNumPendingJobs + " ready jobs");
              if (!checkMemoryUsage()) {
                targetNumPendingJobsPerWorker = 1;
              }
              lastPrintTime = elapsed;
            }
          }
        }
        long launchEnd = System.currentTimeMillis();
        launchingMillis += (launchEnd - launchStart);
      }

      long waitStart = System.currentTimeMillis();
      while (!progressed || (everSaturatedWorkers && completedWorkers.peek() != null)) {
        // process any workers that completed
        AlignerWorker worker = completedWorkers.take();
        boolean succeeded = worker.tryComplete();
        if (succeeded == false) {
          System.out.println("Worker failed; aborting");
          while (completedWorkers.peek() != null) {
            completedWorkers.take().tryComplete();
          }
          return null;
        }

        // remove this worker
        workers.remove(worker);
        pendingWorkers.add(worker);
        long workerSlowestAlignmentMillis = worker.getSlowestAlignmentMillis();
        if (workerSlowestAlignmentMillis > slowestAlignmentMillis) {
          slowestAlignmentMillis = workerSlowestAlignmentMillis;
          slowestQuery = worker.getSlowestQuery();
          slowestAlignment = worker.getSlowestAlignment();
        }
        cpuMillisSpentOnUnalignedQueries += worker.getMillisSpentOnUnalignedQueries();
        cpuMillisSpentAligningMatches += worker.getMillisSpentAligningMatches();
        cpuMillisThroughOptimisticBestAlignments += worker.getMillisThroughOptimisticBestAlignments();

        if (randomMomentSelector.select(System.currentTimeMillis())) {
          Query random = worker.getQueryAtRandomMoment();
          if (random != null)
            queryAtRandomMoment = random;
        }
        numCacheHits += worker.getNumCacheHits();
        numCasesImmediatelyAcceptingFirstAlignment += worker.getNumCasesImmediatelyAcceptingFirstAlignment();
        numIndels += worker.getNumIndels();
        progressed = true;
      }
      long waitEnd = System.currentTimeMillis();
      waitingMillis += (waitEnd - waitStart);
    }
    for (AlignerWorker worker: pendingWorkers) {
      worker.noMoreQueries();
    }
    long doneAligningQueriesAt = System.currentTimeMillis();
    AlignmentStatistics result = new AlignmentStatistics();
    result.millisReadingQueries = readingMillis;
    result.millisLaunchingWorkers = launchingMillis;
    result.millisWaitingForWorkers = waitingMillis;
    result.cpuMillisSpentOnUnalignedQueries = cpuMillisSpentOnUnalignedQueries;
    if (slowestQuery != null) {
      result.slowestQuery = slowestQuery;
      result.slowestQueryNumAlignments = slowestAlignment.getTotalOfAllComponents();
      result.slowestQueryMillis = slowestAlignmentMillis;
    }
    result.queryAtRandomMoment = queryAtRandomMoment;
    result.cpuMillisSpentAligningMatches = cpuMillisSpentAligningMatches;
    result.cpuMillisThroughOptimisticBestAlignments = cpuMillisThroughOptimisticBestAlignments;
    result.numCasesImmediatelyAcceptingFirstAlignment = numCasesImmediatelyAcceptingFirstAlignment;
    result.numQueriesLoaded = numQueriesLoaded;
    result.numCacheHits = numCacheHits;
    result.numIndels = numIndels;

    return result;
  }

  private static boolean checkMemoryUsage() {
    Runtime runtime = Runtime.getRuntime();
    long maxAllowedMemory = runtime.maxMemory();
    long usedMemory = runtime.totalMemory() - runtime.freeMemory();
    double usageFraction = (double)usedMemory / (double)maxAllowedMemory;
    if (usageFraction >= 0.9) {
      long maxAllowedMemoryMB = maxAllowedMemory / 1024 / 1024;
      long usedMemoryMB = usedMemory / 1024 / 1024;
      int usagePercent = (int)(usageFraction * 100);
      System.out.println("Low memory! " + usagePercent + "% used (" + usedMemoryMB + "M/" + maxAllowedMemoryMB + "M). Try larger Xmx");
      return false;
    }
    return true;
  }

  public static List<Sequence> sortAndComplementReference(SequenceProvider provider) {
    Map<Integer, List<Sequence>> sequencesByLength = new TreeMap<Integer, List<Sequence>>();
    while (true) {
      SequenceBuilder builder = provider.getNextSequence();
      if (builder == null)
        break;
      Sequence sequence = builder.build();
      int key = sequence.getLength() * -1;
      List<Sequence> sequencesHere = sequencesByLength.get(key);
      if (sequencesHere == null) {
        sequencesHere = new ArrayList<Sequence>();
        sequencesByLength.put(key, sequencesHere);
      }
      sequencesHere.add(sequence);
      sequencesHere.add(sequence.reverseComplement());
    }
    List<Sequence> sorted = new ArrayList<Sequence>();
    for (int length : sequencesByLength.keySet()) {
      sorted.addAll(sequencesByLength.get(length));
    }
    return sorted;
  }

  // Given that we've completed <count> units of work, tells when to report again
  public static long determineNextCountToReport(long count) {
    // Only output numbers ending with lots of zeros and starting with up to two nonzero digits
    long multiplier = 1;
    while (count > 99) {
      count /= 10;
      multiplier *= 10;
    }
    return (count + 1) * multiplier;
  }
}
