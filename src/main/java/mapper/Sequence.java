package mapper;

import java.util.Arrays;
import java.util.List;

// A Sequence is a list of genomic base pairs: A,C,G,T
public class Sequence {
  public Sequence(String name, String packedContents, int length, String path) {
    this.name = name;
    this.packedContents = packedContents;
    this.length = length;
    this.path = path;
  }

  // returns the name of this sequence
  public String getName() {
    return this.name;
  }

  // returns the name of the sequence that this sequence came from, or its own name if it didn't come from another sequence
  public String getSourceName() {
    return this.getName();
  }

  public String getPath() {
    return this.path;
  }

  // an internal unique identifier that is shorter than getName
  public long getId() {
    return this.identifier;
  }

  public void setId(long identifier) {
    this.identifier = identifier;
  }

  public String getRange(int startIndex, int count) {
    StringBuilder builder = new StringBuilder();
    int endIndex = startIndex + count;
    for (int i = startIndex; i < endIndex; i++) {
      builder.append(this.charAt(i));
    }
    return builder.toString();
  }

  public char charAt(int index) {
    byte basePair = encodedCharAt(index);
    return Basepairs.decode(basePair);
  }

  public byte encodedCharAt(int index) {
    byte[] decompressedContents = this.decompressedContents;
    if (decompressedContents != null)
      return decompressedContents[index];
    return this.computeEncodedCharAt(index);
  }

  protected byte computeEncodedCharAt(int index) {
    // character index (16 bits per character and 4 bits per basepair, so 4 basepairs per character)
    int characterIndex = index >> 2;
    if (characterIndex < 0) {
      throw new IllegalArgumentException("computeEncodedCharAt(" + index + ") attempting to access encoded character at " + characterIndex);
    }
    // offset within character (4 bits per basepair and 4 basepairs per character)
    int offsetInCharacter = (index & 3) << 2;

    // the character to extract bits from
    char character = this.packedContents.charAt(characterIndex);

    // result
    byte result = (byte)((character >> offsetInCharacter) & 15);
    return result;
  }

  public void decompress() {
    byte[] decompressed = new byte[this.length];
    for (int i = 0; i < this.length; i++) {
      decompressed[i] = this.computeEncodedCharAt(i);
    }
    this.decompressedContents = decompressed;
  }
  public void compress() {
    this.decompressedContents = null;
    Sequence complementedFrom = this.getComplementedFrom();
    if (complementedFrom != null)
      complementedFrom.compress();
  }

  public Sequence getSubsequence(int startIndex, int count) {
    if (startIndex == 0 && count == this.getLength()) {
      return this;
    }
    return new Subsequence(this, startIndex, count);
  }

  public String getText() {
    return this.getRange(0, this.getLength());
  }

  public int getLength() {
    return this.length;
  }

  public Sequence reverseComplement() {
    Sequence reverseComplement = new ReverseComplementSequence(this);
    if (this.decompressedContents != null)
      reverseComplement.decompress();
    return reverseComplement;
  }

  // returns the Sequence that this one was created as the reverseComplement of, if any
  public Sequence getComplementedFrom() {
    return null;
  }

  public int compareTo(Sequence other) {
    return Long.compare(this.identifier, other.identifier);
  }

  public int getContentHash() {
    return this.packedContents.hashCode();
  }

  public boolean textEquals(Sequence other) {
    if (this.getLength() != other.getLength())
      return false;
    for (int i = 0; i < this.getLength(); i++) {
      if (this.encodedCharAt(i) != other.encodedCharAt(i))
        return false;
    }
    return true;
  }

  private String name;
  private String packedContents;
  private byte[] decompressedContents;
  private long identifier;
  private int length;
  private String path;
}
