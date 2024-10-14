package mapper;

import java.util.ArrayList;
import java.util.List;
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
    ByteBlockStore store = new Medium_ByteBlockStore(bytesPerBlock);

    // Encode some positions, save them into the ByteBlockStore, and check them
    int blockIndex = 0;
    SequencePosition[] positions = new SequencePosition[0];
    for (int i = 0; i < reference.size(); i++) {
      // encode position as long
      int offset = reference.get(i).getLength() * (i % 4) / 3;
      if (offset >= reference.get(i).getLength())
        offset--;
      SequencePosition position = new SequencePosition(reference.get(i), offset);
      long encoded = database.encodePosition(position.getSequence(), position.getStartIndex());
      // write encoded long to the store
      blockIndex = database.writeEncodedPosition(store, 0, i, encoded);
      // unpack the written positions
      int numBytes = database.getEncodedLength(i + 1);
      SequencePosition[] newDecoded = database.unpackPositions(store, blockIndex, numBytes);

      // compare values
      if (newDecoded.length != positions.length + 1) {
        fail("Added encoded position " + encoded + " to array of length " + positions.length + " but decoded array has length " + positions.length);
      }
      for (int j = 0; j < positions.length; j++) {
        if (!newDecoded[j].equals(positions[j])) {
          fail("Added encoded position " + encoded + " and it changed the encoded value at position " + j + " from " + positions[j] + " to " + newDecoded[j]);
        }
      }
      if (!newDecoded[newDecoded.length - 1].equals(position)) {
        fail("Added position at sequences[" + i + "][" + offset + "] with encoded value " + encoded + " but was unpacked as " + newDecoded[newDecoded.length - 1] + " (maxEncodableValue = " + database.maxEncodableValue + ")");
      }
      positions = newDecoded;
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
    return new RepeatingSequence("" + identifier, 'A', length);
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
