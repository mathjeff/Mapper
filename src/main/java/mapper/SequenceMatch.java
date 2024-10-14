package mapper;

// A SequenceMatch says that if one sequence if shifted by a certain amount then it resembles another one.
public class SequenceMatch {
  public SequenceMatch(Sequence sequenceA, Sequence sequenceB, int offset) {
    this.sequenceA = sequenceA;
    this.sequenceB = sequenceB;
    this.offset = offset;
  }
  public Sequence getSequenceA() {
    return this.sequenceA;
  }
  public Sequence getSequenceB() {
    return this.sequenceB;
  }
  public int getStartIndexB() {
    return Math.max(0, offset);
  }
  public int getEndIndexB() {
    return Math.min(offset + sequenceA.getLength(), sequenceB.getLength());
  }
  public int getStartIndexA() {
    return getStartIndexB() - offset;
  }
  public int getEndIndexA() {
    return getEndIndexB() - offset;
  }
  public int getLength() {
    return getEndIndexB() - getStartIndexB();
  }
  public String getTextA() {
    return this.sequenceA.getRange(getStartIndexA(), this.getLength());
  }
  public String getTextB() {
    return this.sequenceB.getRange(getStartIndexB(), this.getLength());
  }
  public int getOffset() {
    return offset;
  }

  @Override
  public boolean equals(Object otherObject) {
    SequenceMatch other = (SequenceMatch)otherObject;
    if (other == null)
      return false;
    if (this.offset != other.offset)
      return false;
    if (sequenceA != other.sequenceA)
      return false;
    if (sequenceB != other.sequenceB)
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    return offset;
  }

  public boolean getReversed() {
    return (this.sequenceA.getComplementedFrom() != null);
  }

  public String summarizePositionB() {
    return this.sequenceB.getName() + " offset " + this.offset;
  }

  public String summarize() {
    return this.sequenceA.getName() + " at " + this.summarizePositionB();
  }

  public Sequence sequenceA;
  public Sequence sequenceB;
  public int offset;
  // Whether this was generated from a matching subsequence
  // If false, this was guessed in another way (probably from the paired-end sequence mate's offset)
  public boolean fromHashblockMatch = true;
}
