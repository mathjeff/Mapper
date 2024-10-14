package mapper;

public class HashBlock_Compiler implements HashBlock_Row {

  public HashBlock_Compiler(HashBlock_Row sourceRow) {
    this.sourceRow = sourceRow;
    this.cache = HashBlock_CompilerCache.getInstance(sourceRow.getLevel());
  }

  public HashBlock_Compiler(HashBlock_Row sourceRow, HashBlock_CompilerCache cache) {
    this.sourceRow = sourceRow;
    this.cache = cache;
  }

  public IMultiHashBlock get(int index) {
    IMultiHashBlock next = this.getAfter(index - 1);
    if (next != null && next.getStartIndex() == index) {
      return next;
    }
    return null;
  }

  public IMultiHashBlock getAfter(int index) {
    if (index == this.previousRequestIndex) {
      return this.previousResult;
    }
    this.previousResult = this.computeAfter(index);
    this.previousRequestIndex = index;
    return this.previousResult;
  }

  private IMultiHashBlock computeAfter(int index) {
    Sequence sequence = this.getSequence();
    HashBlock_CompilerNode node = this.cache.rootNode;
    int endIndex = index + 1;
    int last = sequence.getLength();
    IMultiHashBlock pendingBlock = null;
    int newRequestShift = -1;
    int prevEnd = -1;
    if (this.previousResultNode != null) {
      newRequestShift = index - this.previousRequestIndex;
    }
    if (newRequestShift > 0) {
      // the previous request was at an earlier index so we might be able to reuse some information
      HashBlock_CompilerNode prevNode = this.previousResultNode.getPrevious(newRequestShift);
      if (prevNode != null && this.previousResult != null) {
        endIndex = this.previousResult.getEndIndex();
        node = prevNode;
        if (node.getBlock() != null) {
          this.previousResultNode = node;
          return node.getBlock().withEnd(endIndex);
        }
      }
      prevEnd = this.previousResult.getEndIndex();
    }
    while (true) {
      boolean ambiguous = false;
      if (endIndex == prevEnd) {
        this.previousResultNode.putPrevious(newRequestShift, node);
      }
      if (endIndex >= last) {
        this.previousResultNode = null;
        return null;
      }
      char c = sequence.charAt(endIndex);
      if (Basepairs.isAmbiguous(c)) {
        ambiguous = true;
      }
      HashBlock_CompilerNode nextNode = node.getNext(c);
      endIndex++;
      if (nextNode == null) {
        if (pendingBlock == null) {
          this.sourceRow.skipTo(index);
          pendingBlock = this.sourceRow.getAfter(index);
          if (pendingBlock != null && pendingBlock.getSingle() == null) {
            // If this is a MultiHashBlock, then we don't know how much area around it can affect its state
            // So, if we see a MultiHashBlock, we don't cache it here
            this.previousResultNode = null;
            return pendingBlock;
          }
        }
        if (ambiguous) {
          // If we encounter ambiguity, then we don't know how much area around this can affect the state
          // So, if we see ambiguity, we don't cache it here
          this.previousResultNode = null;
          return pendingBlock;
        }
        IMultiHashBlock blockHere;
        if (pendingBlock != null && pendingBlock.getEndIndex() == endIndex) {
          blockHere = pendingBlock;
        } else {
          blockHere = null;
        }
        nextNode = new HashBlock_CompilerNode(blockHere);
        node.put(c, nextNode); // it is possible for this to overwrite another node added by another thread, but this is just a cache so it's ok if we lose some nodes occasionally
      }
      node = nextNode;
      if (node.getBlock() != null) {
        this.previousResultNode = node;
        return node.getBlock().withEnd(endIndex);
      }
    }
  }

  public Sequence getSequence() {
    return this.sourceRow.getSequence();
  }

  public void garbageCollect(int index) {
    this.sourceRow.garbageCollect(index);
  }

  public int getLevel() {
    return sourceRow.getLevel();
  }  

  public void skipTo(int index) {
    this.sourceRow.skipTo(index);
  }

  HashBlock_Row sourceRow;
  HashBlock_CompilerCache cache;
  int previousRequestIndex = -2;
  IMultiHashBlock previousResult;
  HashBlock_CompilerNode previousResultNode;
}
