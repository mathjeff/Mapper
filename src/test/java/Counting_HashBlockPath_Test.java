package mapper;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class Counting_HashBlockPath_Test {
  public Counting_HashBlockPath_Test() {
  }

  @Test
  public void checkEfficientlyHandlesRepetitionInQuery() {
    String query     = "GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG";
    String reference = "GGGGGGGGACGTTGCAAACCGGTTATGCTGCAAATTGGCC";

    Counting_HashBlockPath path = makePath(query, reference);

    int numOffsets = path.findGoodPositionsHavingPriorityUpTo(query.length()).size();
    if (numOffsets != 0) {
      fail("number of offsets checked is " + numOffsets + ". Does repetition in the query cause us to check for alignments at an inefficiently large number of offsets?");
    }
  }

  @Test
  public void checkOneHashblockMatchSufficientNearEndOfReference() {
    String query     =             "CCCTTAAGGACCGTGTGAGAACGAC";
    String reference = "ACGTAAGTACGAGCCGTAAGGTCCC";
    //                              ! !      !  !

    Counting_HashBlockPath path = makePath(query, reference);

    int expectedOffset = 12;
    List<HashBlockMatch_Counter> matchingCounters = path.findGoodPositionsHavingPriorityUpTo(query.length());
    if (!containsOffset(matchingCounters, expectedOffset)) {
      fail("didn't find offset " + expectedOffset);
    }
  }

  @Test
  public void checkRepeatedHashblockMatchInsufficientEvenNearEndOfReference() {
    String query     = "ACCC";
    String reference = "ACCCACCCACCCACCCACCC";

    Counting_HashBlockPath path = makePath(query, reference);

    List<HashBlockMatch_Counter> matchingCounters = path.findGoodPositionsHavingPriorityUpTo(query.length());
    if (matchingCounters.size() > 0) {
      String message = "Expected 0 interesting offsets but found " + matchingCounters.size() + ":";
      for (HashBlockMatch_Counter counter: matchingCounters) {
        message += " offset " + counter.getMatch().getOffset();
      }
      fail(message);
    }
  }

  private boolean containsOffset(List<HashBlockMatch_Counter> counters, int offset) {
    for (HashBlockMatch_Counter counter: counters) {
      if (counter.getMatch().getOffset() == offset)
        return true;
    }
    return false;
  }

  private Counting_HashBlockPath makePath(String queryText, String referenceText) {
    Sequence query = new SequenceBuilder().setName("query").add(queryText).build();
    Sequence reference = new SequenceBuilder().setName("reference").add(referenceText).build();
    SequenceDatabase sequenceDatabase = new SequenceDatabase(reference, true);
    Logger logger = new Logger(new StderrWriter());
    HashBlock_Pyramid queryPyramid = new HashBlock_Pyramid(new HashBlock_Stream(query, false, null));
    HashBlock_Database database = new HashBlock_Database(sequenceDatabase);
    AlignmentParameters parameters = new AlignmentParameters();
    parameters.DeletionExtension_Penalty = 0.1;
    Counting_HashBlockPath path = new Counting_HashBlockPath(queryPyramid, database.getView(), sequenceDatabase, query, "query", logger, parameters);
    return path;
  }

  private void fail(String message) {
    Assert.fail(message);
  }
}
