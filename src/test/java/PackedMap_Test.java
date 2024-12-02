package mapper;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;


public class PackedMap_Test {
  public PackedMap_Test() {
  }

  @Test
  public void testLargeReferenceSize() {
    List<Sequence> sequences = makeSequences(8, (int)Math.pow(2, 31));
    Sequence firstSequence = sequences.get(0);
    SequenceDatabase sequenceDatabase = new SequenceDatabase(sequences, true);
    int keyCapacity = 10;
    PackedMap packedMap = new PackedMap(5, keyCapacity, sequenceDatabase);
    List<HashBlock> blocks = new ArrayList<HashBlock>();
    for (int i = 0; i < keyCapacity * 2; i++) {
      int forwardHash = i % keyCapacity;
      int reverseHash = -forwardHash - 1;
      blocks.add(new HashBlock(i, 1, forwardHash, reverseHash));
    }
    packedMap.add(firstSequence, blocks, false);
    for (int i = 0; i < keyCapacity; i++) {
      SequencePosition[] lookupResults = packedMap.get(i);
      if (lookupResults.length != 2) {
        fail("Looked up key " + i + " and expected 2 results, not " + lookupResults.length);
      }
      int expected0 = (i + 0 * keyCapacity);
      int expected1 = (i + 1 * keyCapacity);
      int actual0 = lookupResults[0].getStartIndex();
      int actual1 = lookupResults[1].getStartIndex();
      if (actual1 < actual0) {
        int temp = actual1;
        actual1 = actual0;
        actual0 = temp;
      }
      if (expected0 != actual0) {
        fail("Looked up key " + i + " and got lookup result " + actual0 + ", not " + expected0);
      }
      if (expected1 != actual1) {
        fail("Looked up key " + i + " and got lookup result " + actual1 + ", not " + expected1);
      }
    }
  }

  private List<Sequence> makeSequences(int numSequences, int sequenceLength) {
    // Make SequenceDatabase
    List<Sequence> reference = new ArrayList<Sequence>();
    for (int i = 0; i < numSequences; i++) {
      reference.add(makeSequence(i, sequenceLength));
    }
    return reference;
  }

  private Sequence makeSequence(int identifier, int length) {
    return new RepeatingSequence("" + identifier, 'A', length);
  }

  private void fail(String message) {
    Assert.fail(message);
  }
}
