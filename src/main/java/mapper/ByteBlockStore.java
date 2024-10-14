package mapper;

// Stores byte arrays keyed by index with less memory overhead than an array of arrays
// Items can be cleared, which allows the keys to subsequently be recycled
public abstract class ByteBlockStore {

  public static ByteBlockStore create(long maxNumBlocks, int bytesPerBlock) {
    // if each block is small, we can pack the bytes directly into the key
    if (bytesPerBlock <= 4)
      return new Short_ByteBlockStore(bytesPerBlock);
    // if the number of blocks is large, we need to be careful to hold that many blocks
    if (maxNumBlocks * (long)bytesPerBlock > (long)Integer.MAX_VALUE / 2)
      return new Large_ByteBlockStore(bytesPerBlock);
    // this is supposed to be the common case
    return new Medium_ByteBlockStore(bytesPerBlock);
  }

  public ByteBlockStore(int bytesPerBlock) {
    this.bytesPerBlock = bytesPerBlock;
  }

  // stores the given bytes and returns an index	
  public abstract int put(byte[] bytes);

  public abstract void clear(int index);

  // Fetches blocks[blockIndex], updates the value at blockIndex to equal <value>, saves the result, and returns the index of the resulting block
  // Most of the time this will be faster than calling put get, clear, and put
  public abstract int write(int blockIndex, int byteIndex, byte value);

  // Returns a byte array containing all of the contents at this index
  public byte[] get(int index) {
    return this.get(index, this.bytesPerBlock);
  }

  // Returns a byte array containing the given length of contents at this index
  public abstract byte[] get(int index, int length);

  public abstract byte getAt(int blockIndex, int index);

  public int getNumBytesPerBlock() {
    return bytesPerBlock;
  }

  protected int bytesToInt(byte[] bytes) {
    int result = 0;
    for (int i = bytes.length - 1; i >= 0; i--) {
      result = result << 8;
      int b = bytes[i];
      if (b < 0) {
        b += 256;
      }
      result += b;
    }
    return result;
  }

  protected byte[] intToBytes(int value, int numBytes) {
    int original = value;
    byte[] result = new byte[numBytes];
    for (int i = 0; i < result.length; i++) {
      result[i] = (byte)(value & 255);
      value = value >> 8;
    }
    return result;
  }

  protected int bytesPerBlock;
}
