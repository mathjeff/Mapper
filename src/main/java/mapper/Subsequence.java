package mapper;

class Subsequence extends Sequence {
  public Subsequence(Sequence original, int start, int length) {
    super(original.getName() + "[" + start + ":" + (start + length) + "]", null, length, original.getPath());
    this.startIndex = start;
    this.original = original;
    // The only thing we use the ID for is to break ties between two different queries that were both read from a file, to make sure that the order we process the queries doesn't affect the output
    // We give this reverse complement sequence the same ID as the original sequence because it's convenient
    this.setId(original.getId());
  }

  @Override
  protected byte computeEncodedCharAt(int index) {
    return this.original.encodedCharAt(index + this.startIndex);
  }

  private Sequence original;
  private int startIndex;
}
