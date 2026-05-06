package mapper;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class SamWriter_Test {
  @Test
  public void simpleTest() {
    String query = "ACGTA";

    String ref  = "ACGTAAAAACCGTAAA";

    String sam = buildSam(query, ref);

    String expectedSam = "query	0	ref	1	255	5M	*	0	5	ACGTA	*	AS:f:0.0\n" +
        "";

    checkSam(sam, expectedSam);
  }

  @Test
  public void pairedEndAlignment() {
    Sequence fwd = new SequenceBuilder().setName("one").add("AACCGGTTAT").build();
    Sequence rev = new SequenceBuilder().setName("two").add("ATACGTACGT").build();
    String ref = "AACCGGTTATAAAAAAAAAAACGTACGTATAAAAAAAAAA";
    Query query = new Query(fwd, rev, 1, 100);
    String sam = buildSam(query, ref);

    String expectedSam =
        "one	99	ref	1	255	10M	ref	21	10	AACCGGTTAT	*	cs:f:0.0	AS:f:0.0\n" +
        "two	147	ref	21	255	10M	ref	1	10	ACGTACGTAT	*	cs:f:0.0	AS:f:0.0\n" +
        "";

    checkSam(sam, expectedSam);
  }

  @Test
  public void oneReadWithMultipleAlignments() {
    String query = "ACGTA";
    String ref = "ACGTAAAAACGTAAAA";

    String sam = buildSam(query, ref);

    String expectedSam =
        "query	0	ref	1	255	5M	*	0	5	ACGTA	*	AS:f:0.0\n" +
        "query	0	ref	9	255	5M	*	0	5	ACGTA	*	AS:f:0.0\n" +
        "";

    checkSam(sam, expectedSam);
  }

  @Test
  public void pairedEndReadWithMultipleAlignments() {
    Sequence fwd = new SequenceBuilder().setName("one").add("ACGTA").build();
    Sequence rev = new SequenceBuilder().setName("two").add("GGGGG").build();
    String ref = "ACGTAAAACCCCCTTTTACGTAAAACCCCC";
    Query query = new Query(fwd, rev, 1, 5);
    String sam = buildSam(query, ref);

    String expectedSam =
        "one	99	ref	18	255	5M	ref	26	5	ACGTA	*	cs:f:0.0	AS:f:0.0\n" +
        "two	147	ref	26	255	5M	ref	18	5	CCCCC	*	cs:f:0.0	AS:f:0.0\n" +
        "one	99	ref	1	255	5M	ref	9	5	ACGTA	*	cs:f:0.0	AS:f:0.0\n" +
        "two	147	ref	9	255	5M	ref	1	5	CCCCC	*	cs:f:0.0	AS:f:0.0\n" +
        "";

    checkSam(sam, expectedSam);
  }

  @Test
  public void pairedEndAlignmentOnlyOneSequenceAligned() {
    Sequence fwd = new SequenceBuilder().setName("one").add("AACCGGTTAT").build();
    Sequence rev = new SequenceBuilder().setName("two").add("CCCCCCCCCC").build();
    String ref = "AACCGGTTATAAAAAAAAAAACGTACGTATAAAAAAAAAA";
    Query query = new Query(fwd, rev, 1, 100);
    String sam = buildSam(query, ref);

    String expectedSam =
        "one	73	ref	1	255	10M	*	0	10	AACCGGTTAT	*	cs:f:0.0	AS:f:0.0\n" +
        "";

    checkSam(sam, expectedSam);
  }

  private void checkSam(String actual, String expected) {
    if (!(expected.equals(actual))) {
      String actualAsCode = "\"" + actual.replace("\"", "\\\"").replace("\n", "\\n\" +\n        \"") + "\";";
      fail("Difference in generated .sam file.\nactual sam:\n" + actual + "\nexpected sam:\n" + expected + "\n code for actual sam:\n" + actualAsCode);
    }
  }

  private String buildSam(String queryText, String referenceText) {
    Sequence query = new SequenceBuilder().setName("query").add(queryText).build();
    return buildSam(new Query(query), referenceText);
  }

  private String buildSam(Query query, String referenceText) {
    return buildSam(query, referenceText, makeParameters());
  }

  private AlignmentParameters makeParameters() {
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

  private String buildSam(Query query, String referenceText, AlignmentParameters parameters) {
    Logger logger = new Logger(new StdoutWriter());
    StatusLogger statusLogger = new StatusLogger(logger, System.currentTimeMillis());

    Sequence reference = new SequenceBuilder().setName("ref").add(referenceText).build();
    SequenceDatabase sequenceDatabase = new SequenceDatabase(reference, true);
    HashBlock_Database hashblockDatabase = new HashBlock_Database(sequenceDatabase);

    DuplicationDetector duplicationDetector = new DuplicationDetector(hashblockDatabase, 1, 2, 2, 1, null, statusLogger);
    AlignmentCache resultsCache = new AlignmentCache();

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    List<AlignmentListener> alignmentListeners = new ArrayList<AlignmentListener>();

    AlignerWorker worker = new AlignerWorker(hashblockDatabase, parameters, duplicationDetector.getView(logger), 0, alignmentListeners, resultsCache, new ArrayDeque<AlignerWorker>());
    worker.setup();
    worker.setLogger(logger);
    worker.beforeBatch();
    List<QueryAlignments> results = new ArrayList<QueryAlignments>();
    results.add(worker.align(query));
    worker.afterBatch();
    SamWriter samWriter = new SamWriter(sequenceDatabase, outputStream, false);
    samWriter.addAlignments(results);

    return removeSamHeader(outputStream.toString());
  }

  private String removeSamHeader(String sam) {
    String[] samLines = sam.split("\n");
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < samLines.length; i++) {
      if (samLines[i].startsWith("@") || samLines[i].length() < 1)
        continue;
      result.append(samLines[i]);
      result.append("\n");
    }
    return result.toString();
  }


  private void fail(String message) {
    Assert.fail(message);
  }

}
