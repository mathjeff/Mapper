package mapper;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class SequenceDatabase {
  public SequenceDatabase(Collection<Sequence> sequences) {
    for (Sequence sequence : sequences) {
      add(sequence);
    }
    this.computeMetrics();
  }

  public SequenceDatabase(Collection<Sequence> sequences, boolean addReverseComplement) {
    for (Sequence sequence : sequences) {
      add(sequence);
      if (addReverseComplement) {
        add(sequence.reverseComplement());
      }
    }
    this.computeMetrics();
  }

  public SequenceDatabase(Sequence sequence) {
    this.add(sequence);
    this.computeMetrics();
  }

  public SequenceDatabase(Sequence sequence, boolean addReverseComplement) {
    this.add(sequence);
    if (addReverseComplement)
      add(sequence.reverseComplement());
    this.computeMetrics();
  }

  private void add(Sequence sequence) {
    sequence.setId(this.sequences.size());
    this.sequences.add(sequence);
    if (sequence.getComplementedFrom() == null) {
      this.totalForwardSize += sequence.getLength();
    }
    this.totalForwardAndReverseSize += sequence.getLength();
  }
  public Sequence get(short sequenceId) {
    return this.sequences.get(sequenceId);
  }
  public List<Sequence> getAll() {
    return sequences;
  }
  public List<Sequence> getForwardSequencesOnly() {
    List<Sequence> results = new ArrayList<Sequence>();
    for (Sequence sequence: this.sequences) {
      if (sequence.getComplementedFrom() == null)
        results.add(sequence);
    }
    return results;
  }
  private void computeMetrics() {
    computeEncodingSize();
    computeSequenceEncodingStarts();
  }
  private void computeEncodingSize() {
    int numBitsRequired = log2RoundUp(this.totalForwardAndReverseSize);
    // we're going to pack this into a byte array, so we need to use at least 8 bits per item so we can determine how many items we have
    this.numBitsPerPosition = Math.max(numBitsRequired, 8);
    this.maxEncodableValue = (long)(1L << (long)this.numBitsPerPosition) - 1;
  }

  public int log2RoundUp(long value) {
    int numBits = 1;
    long exponent = 2;
    while (true) {
      if (exponent >= value) {
        return numBits;
      }
      numBits++;
      exponent *= 2;
    }
  }

  public int getNumBitsPerPosition() {
    return this.numBitsPerPosition;
  }

  public int getDecodedLength(byte[] encoded) {
    return getDecodedLength(encoded.length);
  }

  public int getDecodedLength(int numBytes) {
    int numBytesPerPosition = (this.numBitsPerPosition + 7) / 8;
    return numBytes / numBytesPerPosition;
  }

  public int getEncodedLength(int itemLength) {
    int numBytesPerPosition = (this.numBitsPerPosition + 7) / 8;
    return itemLength * numBytesPerPosition;
  }

  // decodes the given byte array into an array of positions
  public SequencePosition[] unpackPositions(ByteKeyStore store, int blockIndex) {
    BytesView bytes = store.get(blockIndex);
    if (bytes == null)
      return null;
    int numBytes = bytes.size();
    int numBytesPerPosition = (this.numBitsPerPosition + 7) / 8;
    int numPositions = numBytes / numBytesPerPosition;
    SequencePosition[] result = new SequencePosition[numPositions];
    int writeIndex = 0;
    long currentValue = 0;
    int numPendingBits = 0;
    for (int readIndex = 0; readIndex < numBytes; readIndex++) {
      long thisByte = bytes.get(readIndex);
      if (thisByte < 0) {
        thisByte += 256;
      }
      //System.err.println("Read encoded[" + readIndex + "] = " + thisByte);
      currentValue = (long)(thisByte << (long)numPendingBits) + currentValue;
      numPendingBits += 8;
      if (numPendingBits >= this.numBitsPerPosition) {
        numPendingBits = 0;
        long currentPosition = currentValue & this.maxEncodableValue;
        SequencePosition decoded = decodePosition(currentPosition);

        Sequence sequence = decoded.getSequence();
        int startIndex = decoded.getStartIndex();
        if (startIndex < 0 || startIndex >= sequence.getLength()) {
          int sequenceIndex = (int)sequence.getId();
          String errorMessage = "In block " + blockIndex + ", failed to decode position #" + writeIndex + " of " + numPositions + " (ending at byte #" + readIndex + ") with value " + currentPosition + " (from packed value " + currentValue + "): sequenceIndex = " + sequenceIndex + " sequence = " + sequence.getName() + " startIndex = " + startIndex + " but startIndex not valid for sequence.length = " + sequence.getLength() + ".";
          if (writeIndex > 0) {
            SequencePosition previous = result[writeIndex - 1];
            Sequence previousSequence = previous.getSequence();
            errorMessage += " Previous position: sequence " + previousSequence.getId() + " (" + previousSequence.getName() + ") at " + previous.getStartIndex();
          }
          throw new ArrayIndexOutOfBoundsException(errorMessage);
        }

        result[writeIndex] = decoded;
        currentValue = (long)(currentValue >> (numBytesPerPosition * 8));
        writeIndex++;
      }
    }
    return result;
  }

  // Tries to append <newEncoded> to store[blockIndex] and returns whether there was room to add it
  public void appendEncodedPosition(ByteKeyStore store, int blockIndex, long newEncoded) {
    if (newEncoded < 0 || newEncoded > this.maxEncodableValue) {
      throw new IllegalArgumentException("Internal error: encoded position " + newEncoded + " is larger than the maximum supported encoded value " + this.maxEncodableValue);
    }
    //System.err.println("writeEncodedPosition store " + store + " blockIndex " + blockIndex + " existing num items = " + existingNumItems + " numBitsPerPosition " + this.numBitsPerPosition);
    int numBytesPerPosition = (this.numBitsPerPosition + 7) / 8;

    byte[] bytes = new byte[numBytesPerPosition];
    long value = newEncoded;
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte)(value & 255);
      value = value >> 8;
    }
    store.add(blockIndex, bytes);
  }

  // Converts from a long to a SequencePosition
  // Note that if encoded < 0 then it refers to a negative location on the first sequence
  public SequencePosition decodePosition(long encoded) {
    // Because we sort contigs by length, it's possible that this position will more often be on an earlier contig
    // So, we don't want to always do an O(log(n)) binary search, which can be slower if there are lots of small contigs
    // Instead, we start by computing a reasonably small upper bound on the index of the appropriate sequence
    int lowIndex = 0;
    int highIndex = 1;
    while (true) {
      if (highIndex >= this.getNumSequences()) {
        highIndex = this.getNumSequences() - 1;
        break;
      }
      if (this.getSequenceEncodingStart(highIndex) < encoded) {
        lowIndex = highIndex;
        highIndex *= 2;
      } else {
        break;
      }
    }

    // Now we binary search between the low and high indices
    while (highIndex > lowIndex + 1) {
      int middleIndex = (highIndex + lowIndex) / 2;
      long middle = getSequenceEncodingStart(middleIndex);
      if (middle > encoded) {
        highIndex = middleIndex;
      } else {
        lowIndex = middleIndex;
      }
    }
    int sequenceIndex;
    if (getSequenceEncodingStart(highIndex) <= encoded) {
      sequenceIndex = highIndex;
    } else {
      sequenceIndex = lowIndex;
    }
    Sequence sequence = this.sequences.get(sequenceIndex);
    long offset = encoded - this.getSequenceEncodingStart(sequence);
    return new SequencePosition(sequence, (int)offset);
  }

  public long encodePosition(Sequence sequence, int startIndex) {
    if (startIndex < 0 || startIndex >= sequence.getLength()) {
      throw new IllegalArgumentException("Invalid position " + startIndex + " for sequence " + sequence.getName() + " of length " + sequence.getLength());
    }
    long result = getSequenceEncodingStart(sequence) + startIndex;
    if (result < 0 || result > this.maxEncodableValue) {
      throw new IllegalArgumentException("encoded " + sequence.getName() + "[" + startIndex + "] and received out-of-range result " + result + " outside range 0-" + this.maxEncodableValue);
    }
    return result;
  }

  private long getSequenceEncodingStart(Sequence sequence) {
    return this.getSequenceEncodingStart((int)sequence.getId());
  }

  private long getSequenceEncodingStart(int sequenceId) {
    return this.sequenceEncodingStarts[sequenceId];
  }

  private void computeSequenceEncodingStarts() {
    long total = 0;
    long[] results = new long[this.sequences.size()];
    for (Sequence sequence : this.sequences) {
      results[(int)sequence.getId()] = total;
      total += sequence.getLength();
    }
    this.sequenceEncodingStarts = results;
  }

  public int getNumSequences() {
    return this.sequences.size();
  }

  public Sequence getSequence(int identifier) {
    return this.sequences.get(identifier);
  }

  // the total size of all sequences in this database in the forward direction
  public long getTotalForwardSize() {
    return totalForwardSize;
  }

  public long getTotalForwardAndReverseSize() {
    return totalForwardAndReverseSize;
  }

  public Sequence getReverseComplement(Sequence sequence) {
    Sequence reverseComplement = sequence.getComplementedFrom();
    if (reverseComplement != null)
      return reverseComplement;
    int sequenceId = (int)sequence.getId();
    int complementId = sequenceId + 1 - (sequenceId % 2) * 2;
    reverseComplement = getSequence(complementId);
    if (reverseComplement.getComplementedFrom() != sequence) {
      throw new IllegalArgumentException("Reverse complement of " + sequence + " is unknown to this SequenceDatabase");
    }
    return reverseComplement;
  }

  public Map<String, String> getCacheKeys() {
    Map<String, String> keys = new HashMap<String, String>();
    keys.put("numSequences", "" + this.sequences.size());
    keys.put("totalSize", "" + totalForwardSize);
    keys.put("firstSequenceName", this.sequences.get(0).getName());
    keys.put("paths", this.getPathsString());
    if (this.ancestral)
      keys.put("ancestral", "true");
    return keys;
  }

  public void setAncestral() {
    this.ancestral = true;
  }

  private String getPathsString() {
    TreeSet<String> paths = new TreeSet<String>();
    for (Sequence sequence: sequences) {
      String path = sequence.getPath();
      if (path != null) {
        paths.add(path);
      }
    }
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    boolean first = true;
    for (String path: paths) {
      if (first) {
        first = false;
      } else {
        builder.append(",");
      }
      File file = new File(path);
      String name = file.getName();
      builder.append(file.getName() + ":" + file.lastModified());
    }
    builder.append("}");
    return builder.toString();
  }

  private List<Sequence> sequences = new ArrayList<Sequence>();
  private long totalForwardSize;
  private long totalForwardAndReverseSize;
  int numBitsPerPosition;
  long maxEncodableValue;
  private long[] sequenceEncodingStarts; // Map<Id, encoding start>
  private boolean ancestral;
}
