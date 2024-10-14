package mapper;

// Stores byte arrays keyed by index with less memory overhead than an array of arrays
// Items can be cleared, which allows the keys to subsequently be recycled
public class Medium_ByteBlockStore extends ByteBlockStore {

  // The number of bytes used to identify a block
  static int bytesPerKey = 4;

  public Medium_ByteBlockStore(int bytesPerBlock) {
    super(bytesPerBlock);
    if (bytesPerBlock < 4)
      throw new IllegalArgumentException("Medium_ByteBlockStore requires >= 4 bytes per block. For smaller sizes use Small_ByteBlockStore");
  }

  // stores the given bytes and returns an index	
  public int put(byte[] bytes) {
    // determine where to put the bytes
    int writeIndex = getExistingUnusedIndex();
    if (writeIndex < 0) {
      // write at the end, and rely on the ByteArrayList to auto-expand
      writeIndex = this.getNumBlocks();
      for (int i = 0; i < this.bytesPerBlock; i++) {
        this.write(writeIndex, i, (byte)0);
      }
    }
    // write the bytes
    for (int i = 0; i < bytes.length; i++) {
      this.write(writeIndex, i, bytes[i]);
    }
    for (int i = bytes.length; i < this.bytesPerBlock; i++) {
      this.write(writeIndex, i, (byte)0);
    }
    return writeIndex;
  }

  protected int getExistingUnusedIndex() {
    if (this.gcIndex >= 0) {
      // We do have a location we can reuse
      int writeIndex = this.gcIndex;

      // Get the next position we will be able to reuse
      byte[] newGcBytes = this.get(this.gcIndex, this.bytesPerKey);
      this.gcIndex = this.bytesToInt(newGcBytes);
      return writeIndex;
    }
    return -1;
  }

  public void clear(int index) {
    // Save the previous gcIndex into this position
    // Convert gcIndex to bytes
    byte[] gcAsBytes = this.intToBytes(this.gcIndex, this.bytesPerKey);
    // save gcIndex here
    int startIndex = index * this.bytesPerBlock;
    for (int i = 0; i < 4; i++) {
      this.write(index, i, gcAsBytes[i]);
    }
    // save new gcIndex
    this.gcIndex = index;
  }

  // Fetches blocks[blockIndex], updates the value at blockIndex to equal <value>, saves the result, and returns the index of the resulting block
  // Most of the time this will be faster than calling put get, clear, and put
  public int write(int blockIndex, int byteIndex, byte value) {
    if (blockIndex == this.gcIndex) {
      throw new IllegalArgumentException("write blockIndex = " + blockIndex + " same as gcIndex = " + this.gcIndex + " (byteIndex = " + byteIndex + ", byte = " + value + ")");
    }
    this.bytes.put(blockIndex * this.bytesPerBlock + byteIndex, value);
    return blockIndex;
  }

  // Returns a byte array containing all of the contents at this index
  public byte[] get(int index) {
    return this.get(index, this.bytesPerBlock);
  }

  // Returns a byte array containing the given length of contents at this index
  public byte[] get(int index, int length) {
    int startIndex = index * this.bytesPerBlock;
    byte[] result = new byte[length];
    for (int i = 0; i < length; i++) {
      result[i] = bytes.get(startIndex + i);
    }
    return result;
  }

  public byte getAt(int blockIndex, int index) {
    return this.bytes.get(blockIndex * this.bytesPerBlock + index);
  }

  protected int getNumBlocks() {
    return this.bytes.size() / this.bytesPerBlock;
  }

  int gcIndex = -2;
  ByteArrayList bytes = new ByteArrayList();
}
