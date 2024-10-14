package mapper;

// a ByteArrayList is just an ArrayList<Byte> without the overhead of an extra pointer in each location

public class ByteArrayList {

  public ByteArrayList() {
  }

  private void ensureCapacity(int capacity) {
    if (this.content.length < capacity) {
      //System.out.println("ByteArrayList growing to capacity " + capacity);
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
    if (index >= this.count) {
      throw new IndexOutOfBoundsException("Requested index " + index + " from array of length " + this.count);
    }
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

  private byte[] content = new byte[8];
  private int count;
}
