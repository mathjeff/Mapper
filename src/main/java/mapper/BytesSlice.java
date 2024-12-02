package mapper;

class BytesSlice implements BytesView {
  public BytesSlice(byte[] data, int start, int length) {
    this.data = data;
    this.start = start;
    this.length = length;
  }

  public int size() {
    return this.length;
  }

  public byte get(int index) {
    return this.data[this.start + index];
  }

  public ByteArrayList writable() {
    return new ByteArrayList(this);
  }

  private byte[] data;
  private int start;
  private int length;
}
