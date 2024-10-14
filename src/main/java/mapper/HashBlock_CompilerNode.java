package mapper;

public class HashBlock_CompilerNode {
  public HashBlock_CompilerNode(IMultiHashBlock block) {
    if (block == null)
      this.nexts = new HashBlock_CompilerNode[4];
    this.block = block;
  }

  public HashBlock_CompilerNode getNext(char item) {
    if (this.block != null)
      throw new IllegalArgumentException("called getExisting(" + item + ") on compiler node having nonempty hashblock: " + this.block);
    int index = itemToIndex(item);
    if (index < 0)
      return null;
    return this.nexts[index];
  }

  // Saves the next node in the tree
  // Only allowed if this.block is null
  public void put(char item, HashBlock_CompilerNode next) {
    this.nexts[itemToIndex(item)] = next;
  }

  public IMultiHashBlock getBlock() {
    return this.block;
  }

  public HashBlock_CompilerNode getPrevious(int prefixLength) {
    if (this.block == null)
      throw new IllegalArgumentException("called getPrevious(" + prefixLength + ") on compiler node having empty hashblock");
    // guard against concurrent modification from another thread
    HashBlock_CompilerNode[] currentNexts = this.nexts;
    if (currentNexts == null || prefixLength > currentNexts.length)
      return null;
    return currentNexts[prefixLength - 1];
  }

  // Specifies the a node in a related tree having a different start
  // Only allowed if this.block is not null
  public void putPrevious(int prefixLength, HashBlock_CompilerNode prev) {
    // guard against concurrent modification from another thread
    HashBlock_CompilerNode[] currentNexts = this.nexts;
    boolean overwrite = false;
    if (currentNexts == null || prefixLength > currentNexts.length) {
      currentNexts = new HashBlock_CompilerNode[prefixLength + 1];
      overwrite = true;
    }
    currentNexts[prefixLength - 1] = prev;
    if (overwrite)
      this.nexts = currentNexts;
  }

  private int itemToIndex(char item) {
    if (item == 'A')
      return 0;
    if (item == 'C')
      return 1;
    if (item == 'G')
      return 2;
    if (item == 'T')
      return 3;
    return -1;
  }

  private HashBlock_CompilerNode[] nexts;
  private IMultiHashBlock block;
}
