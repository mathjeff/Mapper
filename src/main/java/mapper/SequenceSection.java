package mapper;

public class SequenceSection {
  public SequenceSection(Sequence sequence, int startIndex, int endIndex) {
    this.sequence = sequence;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public Sequence getSequence() {
    return this.sequence;
  }

  public int getStartIndex() {
    return this.startIndex;
  }

  public int getEndIndex() {
    return this.endIndex;
  }

  public int getLength() {
    return endIndex - startIndex;
  }

  public String format() {
    return sequence.getName() + this.formatRange();
  }

  public String formatRange() {
    if (this.startIndex != 0 || this.endIndex != this.sequence.getLength())
      return "[" + this.startIndex + ":" + this.endIndex + "]";
    else
      return "";
  }

  private Sequence sequence;
  private int startIndex;
  private int endIndex;
}
