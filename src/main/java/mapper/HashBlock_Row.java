package mapper;

public interface HashBlock_Row {
  // gets the HashBlock at this position
  IMultiHashBlock get(int index);

  // returns the next HashBlock after this position
  IMultiHashBlock getAfter(int index);

  Sequence getSequence();

  // specifies that we're not planning to use the block at <index> anymore
  void garbageCollect(int index);

  int getLevel();

  void skipTo(int index);
}
