package mapper;

// A SequencePosition is a position on a Sequence
public class SequencePosition implements Comparable {
  public SequencePosition(Sequence sequence, int startIndex) {
    this.sequence = sequence;
    this.startIndex = startIndex;
  }

  public Sequence getSequence() {
    return this.sequence;
  }

  public int getStartIndex() {
    return this.startIndex;
  }

  public int compareTo(SequencePosition other) {
    int sequenceComparison = this.sequence.compareTo(other.sequence);
    if (sequenceComparison != 0)
      return sequenceComparison;
    return Integer.compare(this.startIndex, other.startIndex);
  }

  @Override
  public int hashCode() {
    return this.startIndex;
  }

  @Override
  public boolean equals(Object otherObject) {
    SequencePosition other = (SequencePosition)otherObject;
    if (other == null)
      return false;
    if (this.startIndex != other.startIndex)
      return false;
    if (this.sequence != other.sequence)
      return false;
    return true;
  }

  @Override
  public int compareTo(Object otherObject) {
    SequencePosition other = (SequencePosition)otherObject;
    if (this.sequence != other.sequence)
      return Long.compare(this.sequence.getId(), other.sequence.getId());
    if (this.startIndex != other.startIndex)
      return Integer.compare(this.startIndex, other.startIndex);
    return 0;
  }

  @Override
  public String toString() {
    return this.sequence.getName() + "[" + this.startIndex + "]";
  }

  private Sequence sequence;
  private int startIndex;
}
