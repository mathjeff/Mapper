package mapper;

class VcfFormatRequest {
  public VcfFormatRequest(Sequence sequence, int startIndex, int length, Alignments alignments, int jobIndex) {
    this.sequence = sequence;
    this.startIndex = startIndex;
    this.length = length;
    this.alignments = alignments;
    this.jobIndex = jobIndex;
  }

  public Sequence sequence;
  public int startIndex;
  public int length;
  public Alignments alignments;
  public int jobIndex;
}
