package mapper;

// a ByteArrayList is just an ArrayList<Byte> without the overhead of an extra pointer in each location

public class ByteArrayList implements BytesView {

  public ByteArrayList() {
    this.content = new byte[8];
  }

  public ByteArrayList(int initialCapacity) {
    this.content = new byte[initialCapacity];
  }

  public ByteArrayList(BytesView content) {
    int size = content.size();
    this.content = new byte[size];
    for (int i = 0; i < size; i++) {
      this.content[i] = content.get(i);
    }
    this.count = size;
  }

  public void ensureCapacity(int capacity) {
    if (this.content.length < capacity) {
      //System.err.println("ByteArrayList growing to capacity " + capacity);
      byte[] newContent = new byte[capacity];
      for (int i = 0; i < this.content.length; i++) {
        newContent[i] = this.content[i];
      }
      this.content = newContent;
    }
  }

  private void ensureBeyondCapacity(int capacity) {
    int newCapacity = capacity * 11 / 10 + 1;
    if (newCapacity < 0) {
      throw new IllegalArgumentException("Cannot increase beyond capacity " + capacity);
    }
    this.ensureCapacity(newCapacity);
  }

  public void add(byte item) {
    if (this.count >= this.content.length) {
      this.ensureBeyondCapacity(this.content.length);
    }
    this.content[this.count] = item;
    this.count++;
  }

  public byte get(int index) {
    return this.content[index];
  }

  public void put(int index, byte value) {
    if (this.content.length <= index) {
      this.ensureBeyondCapacity(index);
    }
    if (this.count <= index) {
      this.count = index + 1;
    }
    this.content[index] = value;
  }

  public int size() {
    return count;
  }

  public int getCapacity() {
    return this.content.length;
  }

  public ByteArrayList writable() {
    return this;
  }

  private byte[] content;
  private int count;
}
