package mapper;

class ReverseComplementSequence extends Sequence {
  public ReverseComplementSequence(Sequence forward) {
    super(forward.getName() + "-rev", null, forward.getLength(), forward.getPath());
    this.complementedFrom = forward;
    // The only thing we use the ID for is to break ties between two different queries that were both read from a file, to make sure that the order we process the queries doesn't affect the output
    // Also, the reverse complement sequence is essentially a different view of the same sequence anyway
    // We give this reverse complement sequence the same ID as the forward sequence because it's convenient
    this.setId(forward.getId());
  }

  @Override
  protected byte computeEncodedCharAt(int index) {
    byte other = complementedFrom.encodedCharAt(this.getLength() - index - 1);
    return Basepairs.complement(other);
  }

  @Override
  public String getSourceName() {
    return this.getComplementedFrom().getName();
  }

  @Override
  public Sequence getComplementedFrom() {
    return this.complementedFrom;
  }

  @Override
  public Sequence reverseComplement() {
    return this.getComplementedFrom();
  }

  private Sequence complementedFrom;
}
