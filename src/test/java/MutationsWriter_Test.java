package mapper;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class MutationsWriter_Test {

  @Test
  public void testNoMutations() {
    String query = "ACGTA";

    String ref  = "ACGTAAAAAAAAAAAA";

    String mutations = buildMutations(query, ref);

    String expectedMutations = "";

    checkMutations(mutations, expectedMutations);
  }

  @Test
  public void testOneMutation() {
    String query = "AACGTT";

    String ref  = "AACGTAAAAA";

    String mutations = buildMutations(query, ref);

    String expectedMutations =
        "ref	6	A	T	1	1\n" +
        "";

    checkMutations(mutations, expectedMutations);
  }

  @Test
  public void testConsecutiveMutations() {
    String query = "ACGTTTAAACCGG";

    String ref  = "ACGTAAAAACCGG";

    String mutations = buildMutations(query, ref);

    String expectedMutations =
        "ref	5	A	T	1	1\n" +
        "ref	6	A	T	1	1\n" +
        "";

    checkMutations(mutations, expectedMutations);
  }

  @Test
  public void testInsertion() {
    String query = "ACGGACTTACGTCGTTAACCACGA";

    String ref  = "ACGCTTACGTCGTTAACCACGA";

    String mutations = buildMutations(query, ref);

    String expectedMutations =
        "ref	3	--	GA	1	1\n" +
        "";

    checkMutations(mutations, expectedMutations);
  }

  @Test
  public void testDeletion() {
    String query = "CACGTAACCGGTTATT";

    String ref  = "CACGTAAGACCGGTTATT";

    String mutations = buildMutations(query, ref);

    String expectedMutations =
        "ref	8	AG	--	1	1\n" +
        "";

    checkMutations(mutations, expectedMutations);
  }

  @Test
  public void testIgnoringMutationWithLowDepth() {
    String query = "ACGTAACTCCGGCTC";

    String ref  = "ACGTACGTCCGGCTC";

    MutationDetectionParameters filter = new MutationDetectionParameters();
    filter.minSNPTotalDepth = 2;

    String filteredMutations = buildMutations(query, ref, filter, 0);
    String expectedFilteredMutations =
        "";
    checkMutations(filteredMutations, expectedFilteredMutations);

    String unfilteredMutations = buildMutations(query, ref);
    String expectedUnfilteredMutations =
        "ref	6	C	A	1	1\n" +
        "ref	7	G	C	1	1\n" +
        "";
    checkMutations(unfilteredMutations, expectedUnfilteredMutations);
  }

  @Test
  public void testIgnoringIndelNearQueryEnd() {
    String query = "CCTAACGTAACTCTGGCCGCAA";

    String ref  = "AGGAACCTACGTAACTCTGGCCGCAA";

    MutationDetectionParameters filter = new MutationDetectionParameters();
    filter.minIndelTotalStartDepth = 1;
    double distinguishQueryEnds = 0.5;

    String filteredMutations = buildMutations(query, ref, filter, distinguishQueryEnds);
    String expectedFilteredMutations =
        "";
    checkMutations(filteredMutations, expectedFilteredMutations);

    String unfilteredMutations = buildMutations(query, ref);
    String expectedUnfilteredMutations =
        "ref	8	-	A	1	1\n" +
        "";
    checkMutations(unfilteredMutations, expectedUnfilteredMutations);
  }

  private void checkMutations(String actual, String expected) {
    actual = withoutMetadataLines(actual);
    if (!(expected.equals(actual))) {
      String actualAsCode = "\"" + actual.replace("\n", "\\n\" +\n        \"") + "\";";
      fail("Difference in generated mutations file.\nactual:\n" + actual + "\nexpected:\n" + expected + "\n code for actual:\n" + actualAsCode);
    }
  }

  private String withoutMetadataLines(String original) {
    String[] lines = original.split("\n");
    StringBuilder resultBuilder = new StringBuilder();
    for (String line: lines) {
      if (!line.startsWith("#") && !line.startsWith("CHR") && line.length() > 0) {
        resultBuilder.append(line);
        resultBuilder.append("\n");
      }
    }
    return resultBuilder.toString();
  }

  private String buildMutations(String queryText, String referenceText) {
    return buildMutations(queryText, referenceText, MutationDetectionParameters.emptyFilter(), 0);
  }

  private String buildMutations(String queryText, String referenceText, MutationDetectionParameters parameters, double queryEndFraction) {
    Sequence querySequence = new SequenceBuilder().setName("query").add(queryText).build();
    Query query = new Query(querySequence);
    return buildMutations(query, referenceText, parameters, queryEndFraction);
  }

  private String buildMutations(Query query, String referenceText, MutationDetectionParameters mutationParameters, double queryEndFraction) {
    Logger logger = new Logger(new StderrWriter());
    StatusLogger statusLogger = new StatusLogger(logger, System.currentTimeMillis());

    Sequence reference = new SequenceBuilder().setName("ref").add(referenceText).build();
    SequenceDatabase sequenceDatabase = new SequenceDatabase(reference, true);
    HashBlock_Database hashblockDatabase = new HashBlock_Database(sequenceDatabase);

    DuplicationDetector duplicationDetector = new DuplicationDetector(hashblockDatabase, 1, 2, 2, 1, null, statusLogger);
    AlignmentCache resultsCache = new AlignmentCache();

    List<AlignmentListener> alignmentListeners = new ArrayList<AlignmentListener>();
    MatchDatabase matchDatabase = new MatchDatabase(queryEndFraction);

    AlignmentParameters parameters = makeAlignmentParameters();
    AlignerWorker worker = new AlignerWorker(hashblockDatabase, parameters, duplicationDetector.getView(logger), 0, alignmentListeners, resultsCache, new ArrayDeque<AlignerWorker>());
    worker.setup();
    worker.setLogger(logger);
    worker.beforeBatch();
    List<QueryAlignments> queryAlignments = new ArrayList<QueryAlignments>();
    queryAlignments.add(worker.align(query));
    matchDatabase.addAlignments(queryAlignments);
    worker.afterBatch();

    Map<Sequence, Alignments> alignments = matchDatabase.groupByPosition();

    ByteArrayOutputStream mutationsStream = new ByteArrayOutputStream();
    MutationsWriter mutationsWriter = new MutationsWriter(mutationsStream, mutationParameters);
    try {
      mutationsWriter.write(alignments, 1);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return mutationsStream.toString();
  }

  private AlignmentParameters makeAlignmentParameters() {
    AlignmentParameters parameters = new AlignmentParameters();
    parameters.MutationPenalty = 1;
    parameters.InsertionStart_Penalty = 1.5;
    parameters.InsertionExtension_Penalty = 0.6;
    parameters.DeletionStart_Penalty = 1.5;
    parameters.DeletionExtension_Penalty = 0.5;
    parameters.MaxErrorRate = 0.2;
    parameters.AmbiguityPenalty = parameters.MaxErrorRate;
    parameters.UnalignedPenalty = parameters.MaxErrorRate;
    return parameters;
  }

  private void fail(String message) {
    Assert.fail(message);
  }
}
