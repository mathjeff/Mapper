package mapper;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class HashBlockPaths_Counter_Test {
  public HashBlockPaths_Counter_Test() {
  }

  @Test
  public void checkComputesDistanceCorrectly() {
    String refText  = "GGGGGACGTGGGGGGAACTAAGGGG";
    String seq1Text =     "GACGTG";
    String seq2Text =                "AACTAAG";
    checkDistance(refText, seq1Text, seq2Text, 5, 18);
  }

  @Test
  public void checkReverseComplementAlignment() {
    String refText  = "GGGGGACGTGGGGGGAACTAAGGGG";
    String seq1Text =     "GACGTG";
    String seq2Text =                "AACTAAG";
    checkDistance(reverseComplement(refText), seq1Text, seq2Text, 5, 18);
  }

  @Test
  public void checkOverlappingDistance() {
    String refText  = "GGGGAACCACTGGGGG";
    String seq1Text =    "GAACCACTG";
    String seq2Text =       "CCACTGGGG";
    checkDistance(refText, seq1Text, seq2Text, -6, 12);
  }

  @Test
  public void checkMultipleMatches() {
    String refText  = "GGGGGAACAGTGGGGGGAACTAAGGGGAATTGTATATAGCG";
    String seq1Text =     "GAACAGTG";
    String seq2Text =                  "AACTAAGGGGAA";
    List<QueryMatch> matches = getMatches(refText + refText, seq1Text, seq2Text);
    if (matches.size() != 2) {
      fail("Expected 2 matches, got " + matches.size());
    }

  }

  private void checkDistance(String refText, String seq1Text, String seq2Text, int innerDistance, int outerDistance) {
    System.err.println("checkDistance ref = '" + refText + "' seq1 = '" + seq1Text + "' seq2 = '" + seq2Text + "'");
    List<QueryMatch> matches = getMatches(refText, seq1Text, seq2Text);
    if (matches.size() != 1) {
      fail("Expected 1 match, got " + matches.size());
    }
    QueryMatch match = matches.get(0);
    if (match.getTotalDistanceBetweenComponents() != innerDistance) {
      fail("Expected total distance between components of " + innerDistance + ", not " + match.getTotalDistanceBetweenComponents());
    }
    if (match.getTotalDistanceAcross() != outerDistance) {
      fail("Expected total distance between components of " + outerDistance + ", not " + match.getTotalDistanceAcross());
    }
  }

  private List<QueryMatch> getMatches(String refText, String seq1Text, String seq2Text) {
    Logger logger = new Logger(new PrintWriter());
    Sequence query1 = new SequenceBuilder().setName("seq1").add(seq1Text).build();
    seq2Text = reverseComplement(seq2Text);
    Sequence query2 = new SequenceBuilder().setName("seq2").add(seq2Text).build();
    Sequence reference = new SequenceBuilder().setName("ref").add(refText).build();

    List<Counting_HashBlockPath> components = new ArrayList<Counting_HashBlockPath>();

    List<Sequence> referenceSequences = new ArrayList<Sequence>();
    referenceSequences.add(reference);
    referenceSequences.add(reference.reverseComplement());
    SequenceDatabase referenceDatabase = new SequenceDatabase(referenceSequences);

    components.add(makePath(query1, referenceDatabase, "fwd-query"));
    components.add(makePath(query2, referenceDatabase, "rev-query"));
    int expectedInnerDistance = 10;
    int maxInnerDistance = 20;
    HashBlockPaths_Counter counter = new HashBlockPaths_Counter(components, expectedInnerDistance, maxInnerDistance, logger);
    List<QueryMatch> matches = counter.findGoodPositionsHavingPriority(0);
    return matches;
  }

  private Counting_HashBlockPath makePath(Sequence query, SequenceDatabase sequenceDatabase, String name) {
    Logger logger = new Logger(new PrintWriter());
    StatusLogger statusLogger = new StatusLogger(logger, 0);
    HashBlock_Pyramid queryPyramid = new HashBlock_Pyramid(new HashBlock_Stream(query, false, null));
    HashBlock_Database database = new HashBlock_Database(sequenceDatabase, statusLogger);
    AlignmentParameters parameters = new AlignmentParameters();
    parameters.DeletionExtension_Penalty = 0.1;
    Counting_HashBlockPath path = new Counting_HashBlockPath(queryPyramid, database.getView(), sequenceDatabase, query, name, logger, parameters);
    return path;
  }

  private String reverseComplement(String input) {
    return new SequenceBuilder().setName("temp").add(input).build().reverseComplement().getText();
  }

  private void fail(String message) {
    Assert.fail(message);
  }
}
