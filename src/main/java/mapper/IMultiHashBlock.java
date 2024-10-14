package mapper;

import java.util.List;

// An IMultiHashBlock is essentially just a MultiHashBlock but can be implemented via a HashBlock to save memory
public interface IMultiHashBlock {
  public HashBlock getSingle();
  public List<ConditionalHashBlock> getPossibilities();

  // the smallest index of any of the HashBlocks in getPossibilities
  public int getStartIndex();

  // the largest index any of the HashBlocks in getPossibilities
  public int getEndIndex();

  // the smallest length of any of the HashBlocks in getPossibilities
  public int getMinLength();

  public String toString(Sequence sequence);

  public IMultiHashBlock withEnd(int endIndex);
}
