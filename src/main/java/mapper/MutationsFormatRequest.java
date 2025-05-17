package mapper;

class MutationsFormatRequest {
  public MutationsFormatRequest(Sequence sequence, int startIndex, int length, FilteredAlignments alignments, int jobIndex) {
    this.sequence = sequence;
    this.startIndex = startIndex;
    this.length = length;
    this.alignments = alignments;
    this.jobIndex = jobIndex;
  }

  public Sequence sequence;
  public int startIndex;
  public int length;
  public FilteredAlignments alignments;
  public int jobIndex;
}
