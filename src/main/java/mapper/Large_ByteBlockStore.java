package mapper;

// Stores byte arrays keyed by index
// Supports larger sizes than a Medium_ByteBlockStore
public class Large_ByteBlockStore extends Medium_ByteBlockStore {

  public Large_ByteBlockStore(int bytesPerBlock) {
    super(bytesPerBlock);
    this.byteArrays = new ByteArrayList[bytesPerBlock];
    for (int i = 0; i < bytesPerBlock; i++) {
      this.byteArrays[i] = new ByteArrayList();
    }
  }


  // Fetches blocks[blockIndex], updates the value at blockIndex to equal <value>, saves the result, and returns the index of the resulting block
  // Most of the time this will be faster than calling put get, clear, and put
  @Override
  public int write(int blockIndex, int byteIndex, byte value) {
    //System.out.println("write blockIndex " + blockIndex + " byteIndex " + byteIndex + " value " + value);
    this.byteArrays[byteIndex].put(blockIndex, value);
    return blockIndex;
  }

  // Returns a byte array containing all of the contents at this index
  @Override
  public byte[] get(int index) {
    return this.get(index, this.bytesPerBlock);
  }

  // Returns a byte array containing the given length of contents at this index
  @Override
  public byte[] get(int index, int length) {
    byte[] result = new byte[length];
    for (int i = 0; i < length; i++) {
      result[i] = this.getAt(index, i);
    }
    return result;
  }

  @Override
  public byte getAt(int blockIndex, int index) {
    ByteArrayList bytes = this.byteArrays[index];
    if (bytes.size() <= blockIndex)
      return (byte)0;
    return bytes.get(blockIndex);
  }

  @Override
  protected int getNumBlocks() {
    return byteArrays[0].size();
  }

  ByteArrayList[] byteArrays;
}
