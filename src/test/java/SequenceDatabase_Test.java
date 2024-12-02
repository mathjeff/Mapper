package mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;


public class SequenceDatabase_Test {
  public SequenceDatabase_Test() {
  }

  @Test
  public void testEncodingLargeSequences() {
    int sequenceSize = (int)Math.pow(2, 30);
    List<Sequence> reference = makeLargeSequences(16, sequenceSize);
    SequenceDatabase database = new SequenceDatabase(reference);
    for (int i = 0; i < reference.size(); i++) {
      testEncoding(database, reference.get(i), 0);
      testEncoding(database, reference.get(i), 100);
      testEncoding(database, reference.get(i), reference.get(i).getLength() - 100);
      testEncoding(database, reference.get(i), reference.get(i).getLength() - 1);
    }
  }

  @Test
  public void testEncodingManyLargeSequences() {
    int numSequences = (int)Math.pow(2, 13);
    int sequenceSize = (int)Math.pow(2, 21);
    List<Sequence> reference = makeLargeSequences(numSequences, sequenceSize);
    SequenceDatabase database = new SequenceDatabase(reference);
    for (int i = 0; i < reference.size(); i++) {
      testEncoding(database, reference.get(i), 0);
      testEncoding(database, reference.get(i), 100);
      testEncoding(database, reference.get(i), reference.get(i).getLength() - 100);
      testEncoding(database, reference.get(i), reference.get(i).getLength() - 1);
    }
  }

  @Test
  public void testPackingPositionsInLargeSequences() {
    int sequenceLengthBits = 21;
    int sequenceSize = (int)Math.pow(2, sequenceLengthBits);

    // Make a SequenceDatabase
    int numSequencesBits = 13;
    int numSequences = (int)Math.pow(2, numSequencesBits);
    List<Sequence> reference = makeLargeSequences(numSequences, sequenceSize);
    SequenceDatabase database = new SequenceDatabase(reference);

    // Make a ByteBlockStore
    int encodingBits = sequenceLengthBits + numSequencesBits;
    int bytesPerPosition = (encodingBits + 7) / 8;
    int numPositions = reference.size();
    int bytesPerBlock = bytesPerPosition * numPositions;
    ByteKeyStore store = new ByteKeyStore(numPositions, bytesPerBlock, encodingBits);

    // Encode some positions, save them into the ByteBlockStore, and check them
    HashSet<SequencePosition> positions = new HashSet<SequencePosition>();
    int nextIndexToCheck = 1;
    for (int i = 0; i < numPositions; i++) {
      // encode position as long
      int offset = reference.get(i).getLength() * (i % 4) / 3;
      if (offset >= reference.get(i).getLength())
        offset--;
      SequencePosition position = new SequencePosition(reference.get(i), offset);
      long encoded = database.encodePosition(position.getSequence(), position.getStartIndex());
      // write encoded long to the store
      database.appendEncodedPosition(store, (byte)0, encoded);
      positions.add(position);


      // Doing so many HashSet lookups can take a substantial amount of time so we only do the comparisons sometimes
      if (i < 1000 || i >= nextIndexToCheck || i == numPositions - 1) {
        nextIndexToCheck = i * 2;

        // unpack the written positions
        int numBytes = database.getEncodedLength(i + 1);
        SequencePosition[] newDecoded = database.unpackPositions(store, 0);

        // compare values
        if (newDecoded == null) {
          fail("Added encoded position " + encoded + " to array of length " + (positions.size() - 1) + " but decoded array is null");
        }
        // check size
        if (newDecoded.length != positions.size()) {
          fail("Added encoded position " + encoded + " to array of length " + (positions.size() - 1) + " but decoded array has length " + newDecoded.length);
        }
        // check no removals
        for (int j = 0; j < newDecoded.length; j++) {
          if (!positions.contains(newDecoded[j])) {
            fail("Decoded position " + newDecoded[j] + " not found in expected decoded positions (last added position = " + position + ")");
          }
        }
        // check no duplicates
        positions = new HashSet<SequencePosition>();
        for (int j = 0; j < newDecoded.length; j++) {
          if (!positions.add(newDecoded[j])) {
            fail("Position " + newDecoded[j] + " found twice (last added position = " + position + ")");
          }
        }
      }
    }
  }

  private List<Sequence> makeLargeSequences(int numSequences, int sequenceLength) {
    // Make SequenceDatabase
    List<Sequence> reference = new ArrayList<Sequence>();
    for (int i = 0; i < numSequences; i++) {
      reference.add(makeLargeSequence(i, sequenceLength - i));
    }
    return reference;
  }

  private Sequence makeLargeSequence(int identifier, int length) {
    return new RepeatingSequence("seq" + identifier, 'A', length);
  }

  private void testEncoding(SequenceDatabase sequenceDatabase, Sequence sequence, int position) {
    long encoded = sequenceDatabase.encodePosition(sequence, position);
    SequencePosition decoded = sequenceDatabase.decodePosition(encoded);
    if (decoded.getSequence() != sequence || decoded.getStartIndex() != position) {
      fail("pre-decoded value " + sequence.getName() + "[" + position + "] != post-decoded value " + decoded.getSequence().getName() + "[" + decoded.getStartIndex() + "]");
    }
  }

  private void fail(String message) {
    Assert.fail(message);
  }
}
