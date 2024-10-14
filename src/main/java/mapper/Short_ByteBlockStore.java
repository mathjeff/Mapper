package mapper;

// Directly encodes the bytes as an integer
public class Short_ByteBlockStore extends ByteBlockStore {

  public Short_ByteBlockStore(int bytesPerBlock) {
    super(bytesPerBlock);
  }

  // stores the given bytes and returns an index	
  public int put(byte[] bytes) {
    // If we can encode the bytes as an index, we just do that
    return this.bytesToInt(bytes);
  }

  public void clear(int index) {
  }

  // Fetches blocks[blockIndex], updates the value at blockIndex to equal <value>, saves the result, and returns the index of the resulting block
  // Most of the time this will be faster than calling put get, clear, and put
  public int write(int blockIndex, int byteIndex, byte value) {
    byte[] existing = this.get(blockIndex);
    existing[byteIndex] = value;
    return this.put(existing);
  }

  // Returns a byte array containing all of the contents at this index
  public byte[] get(int index) {
    return this.get(index, this.bytesPerBlock);
  }

  // Returns a byte array containing the given length of contents at this index
  public byte[] get(int index, int length) {
    return this.intToBytes(index, this.bytesPerBlock);
  }

  public byte getAt(int blockIndex, int index) {
    // If we can encode the bytes as an index, we just do that
    return this.get(blockIndex)[index];
  }
}
