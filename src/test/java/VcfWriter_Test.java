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

public class VcfWriter_Test {
  @Test
  public void simpleTest() {
    String query = "ACGTA";

    String ref  = "ACGTAAAAACCGTAAA";

    String vcf = buildVcf(query, ref, 0);

    String expectedVcf = 
        "ref	1	A	.	1	1,0	0,0\n" +
        "ref	2	C	.	1	1,0	0,0\n" +
        "ref	3	G	.	1	1,0	0,0\n" +
        "ref	4	T	.	1	1,0	0,0\n" +
        "ref	5	A	.	1	1,0	0,0\n" +
        "";

    checkVcf(vcf, expectedVcf);
  }

  @Test
  public void pairedEndAlignment() {
    Sequence fwd = new SequenceBuilder().setName("one").add("AACCGGTTAT").build();
    Sequence rev = new SequenceBuilder().setName("two").add("ATACGTACGT").build();
    String ref = "AACCGGTTATAAAAAAAAAAACGTACGTATAAAAAAAAAA";
    Query query = new Query(fwd, rev, 1, 100);
    String vcf = buildVcf(query, ref, 0);

    String expectedVcf =
        "ref	1	A	.	1	1,0	0,0\n" +
        "ref	2	A	.	1	1,0	0,0\n" +
        "ref	3	C	.	1	1,0	0,0\n" +
        "ref	4	C	.	1	1,0	0,0\n" +
        "ref	5	G	.	1	1,0	0,0\n" +
        "ref	6	G	.	1	1,0	0,0\n" +
        "ref	7	T	.	1	1,0	0,0\n" +
        "ref	8	T	.	1	1,0	0,0\n" +
        "ref	9	A	.	1	1,0	0,0\n" +
        "ref	10	T	.	1	1,0	0,0\n" +
        "ref	21	A	.	1	0,1	0,0\n" +
        "ref	22	C	.	1	0,1	0,0\n" +
        "ref	23	G	.	1	0,1	0,0\n" +
        "ref	24	T	.	1	0,1	0,0\n" +
        "ref	25	A	.	1	0,1	0,0\n" +
        "ref	26	C	.	1	0,1	0,0\n" +
        "ref	27	G	.	1	0,1	0,0\n" +
        "ref	28	T	.	1	0,1	0,0\n" +
        "ref	29	A	.	1	0,1	0,0\n" +
        "ref	30	T	.	1	0,1	0,0\n" +
        "";

    checkVcf(vcf, expectedVcf);
  }

  @Test
  public void oneReadWithMultipleAlignments() {
    String query = "ACGTA";
    String ref = "ACGTAAAAACGTAAAA";

    String vcf = buildVcf(query, ref, 0);

    String expectedVcf =
        "ref	1	A	.	0.5	0.5,0	0,0\n" +
        "ref	2	C	.	0.5	0.5,0	0,0\n" +
        "ref	3	G	.	0.5	0.5,0	0,0\n" +
        "ref	4	T	.	0.5	0.5,0	0,0\n" +
        "ref	5	A	.	0.5	0.5,0	0,0\n" +
        "ref	9	A	.	0.5	0.5,0	0,0\n" +
        "ref	10	C	.	0.5	0.5,0	0,0\n" +
        "ref	11	G	.	0.5	0.5,0	0,0\n" +
        "ref	12	T	.	0.5	0.5,0	0,0\n" +
        "ref	13	A	.	0.5	0.5,0	0,0\n" +
        "";

    checkVcf(vcf, expectedVcf);

  }

  @Test
  public void pairedEndReadWithMultipleAlignments() {
    Sequence fwd = new SequenceBuilder().setName("one").add("ACGTA").build();
    Sequence rev = new SequenceBuilder().setName("two").add("GGGGG").build();
    String ref = "ACGTAAAACCCCCTTTTACGTAAAACCCCC";
    Query query = new Query(fwd, rev, 1, 5);
    String vcf = buildVcf(query, ref, 0);

    String expectedVcf = 
        "ref	1	A	.	0.5	0.5,0	0,0\n" +
        "ref	2	C	.	0.5	0.5,0	0,0\n" +
        "ref	3	G	.	0.5	0.5,0	0,0\n" +
        "ref	4	T	.	0.5	0.5,0	0,0\n" +
        "ref	5	A	.	0.5	0.5,0	0,0\n" +
        "ref	9	C	.	0.5	0,0.5	0,0\n" +
        "ref	10	C	.	0.5	0,0.5	0,0\n" +
        "ref	11	C	.	0.5	0,0.5	0,0\n" +
        "ref	12	C	.	0.5	0,0.5	0,0\n" +
        "ref	13	C	.	0.5	0,0.5	0,0\n" +
        "ref	18	A	.	0.5	0.5,0	0,0\n" +
        "ref	19	C	.	0.5	0.5,0	0,0\n" +
        "ref	20	G	.	0.5	0.5,0	0,0\n" +
        "ref	21	T	.	0.5	0.5,0	0,0\n" +
        "ref	22	A	.	0.5	0.5,0	0,0\n" +
        "ref	26	C	.	0.5	0,0.5	0,0\n" +
        "ref	27	C	.	0.5	0,0.5	0,0\n" +
        "ref	28	C	.	0.5	0,0.5	0,0\n" +
        "ref	29	C	.	0.5	0,0.5	0,0\n" +
        "ref	30	C	.	0.5	0,0.5	0,0\n" +
        "";

    checkVcf(vcf, expectedVcf);
  }

  @Test
  public void testDistinguishQueryEnds() {
    String queryText = "ACCGGGTTTT";
    Sequence querySequence = new SequenceBuilder().setName("query").add(queryText).build();
    Query query = new Query(querySequence);
    String ref = queryText;

    String vcf = buildVcf(query, ref, 0.2);

    String expectedVcf =
        "ref	1	A	.	1	0,0	1,0\n" +
        "ref	2	C	.	1	0,0	1,0\n" +
        "ref	3	C	.	1	1,0	0,0\n" +
        "ref	4	G	.	1	1,0	0,0\n" +
        "ref	5	G	.	1	1,0	0,0\n" +
        "ref	6	G	.	1	1,0	0,0\n" +
        "ref	7	T	.	1	1,0	0,0\n" +
        "ref	8	T	.	1	1,0	0,0\n" +
        "ref	9	T	.	1	0,0	1,0\n" +
        "ref	10	T	.	1	0,0	1,0\n" +
        "";

    checkVcf(vcf, expectedVcf);
  }

  private void checkVcf(String actual, String expected) {
    if (!(expected.equals(actual))) {
      String actualAsCode = "\"" + actual.replace("\"", "\\\"").replace("\n", "\\n\" +\n        \"") + "\";";
      fail("Difference in generated .vcf file.\nactual vcf:\n" + actual + "\nexpected vcf:\n" + expected + "\n code for actual vcf:\n" + actualAsCode);
    }
  }

  private String buildVcf(String queryText, String referenceText, double queryEndFraction) {
    Sequence query = new SequenceBuilder().setName("query").add(queryText).build();
    return buildVcf(new Query(query), referenceText, queryEndFraction);
  }

  private String buildVcf(Query query, String referenceText, double queryEndFraction) {
    return buildVcf(query, referenceText, makeParameters(), queryEndFraction);
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

  private String buildVcf(Query query, String referenceText, AlignmentParameters parameters, double queryEndFraction) {
    Logger logger = new Logger(new StdoutWriter());
    StatusLogger statusLogger = new StatusLogger(logger, System.currentTimeMillis());

    Sequence reference = new SequenceBuilder().setName("ref").add(referenceText).build();
    SequenceDatabase sequenceDatabase = new SequenceDatabase(reference, true);
    HashBlock_Database hashblockDatabase = new HashBlock_Database(sequenceDatabase);

    DuplicationDetector duplicationDetector = new DuplicationDetector(hashblockDatabase, 1, 2, 2, 1, null, statusLogger);
    AlignmentCache resultsCache = new AlignmentCache();

    List<AlignmentListener> alignmentListeners = new ArrayList<AlignmentListener>();
    MatchDatabase matchDatabase = new MatchDatabase(queryEndFraction);

    AlignerWorker worker = new AlignerWorker(hashblockDatabase, parameters, duplicationDetector.getView(logger), 0, alignmentListeners, resultsCache, new ArrayDeque<AlignerWorker>());
    worker.setup();
    worker.setLogger(logger);
    worker.beforeBatch();
    List<QueryAlignments> queryAlignments = new ArrayList<QueryAlignments>();
    queryAlignments.add(worker.align(query));
    matchDatabase.addAlignments(queryAlignments);
    worker.afterBatch();

    Map<Sequence, Alignments> alignments = matchDatabase.groupByPosition();

    ByteArrayOutputStream vcfStream = new ByteArrayOutputStream();
    VcfWriter vcfWriter = new VcfWriter(vcfStream, true);
    try {
      vcfWriter.write(alignments, 1);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return removeVcfHeader(vcfStream.toString());
  }

  private String removeVcfHeader(String vcf) {
    String[] vcfLines = vcf.split("\n");
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < vcfLines.length; i++) {
      if (vcfLines[i].startsWith("#") || vcfLines[i].length() < 1)
        continue;
      result.append(vcfLines[i]);
      result.append("\n");
    }
    return result.toString();
  }


  private void fail(String message) {
    Assert.fail(message);
  }

}
