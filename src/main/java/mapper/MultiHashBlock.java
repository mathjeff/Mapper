package mapper;

import java.util.ArrayList;
import java.util.List;

// A MultiHashBlock represents several alternate HashBlocks
// The reason for this is if a sequence contains ambiguities then the sequence really refers to multiple possible series of base pairs
// A MultiHashBlock refers to a section of HashBlocks for each possible set of underlying base pairs
public class MultiHashBlock implements IMultiHashBlock {
  public MultiHashBlock(List<ConditionalHashBlock> possibilities) {
    this.possibilities = possibilities;
  }

  // used by IMultiHashBlock
  public HashBlock getSingle() {
    return null;
  }
  public List<ConditionalHashBlock> getPossibilities() {
    return this.possibilities;
  }
  public int getStartIndex() {
    int min = -1;
    for (ConditionalHashBlock possibility : this.possibilities) {
      HashBlock block = possibility.getHashBlock();
      if (block != null) {
        int value = block.getStartIndex();
        if (min < 0 || min > value)
          min = value;
      }
    }
    return min;
  }

  public int getEndIndex() {
    int max = -1;
    for (ConditionalHashBlock possibility : this.possibilities) {
      HashBlock block = possibility.getHashBlock();
      if (block != null) {
        int value = block.getEndIndex();
        if (max < value)
          max = value;
      }
    }
    return max;
  }

  public int getMinLength() {
    int min = -1;
    for (ConditionalHashBlock possibility : this.possibilities) {
      HashBlock block = possibility.getHashBlock();
      if (block != null) {
        int value = block.getLength();
        if (min < 0 || min > value)
          min = value;
      }
    }
    return min;
  }

  public IMultiHashBlock withEnd(int index) {
    int shift = index - this.getEndIndex();
    List<ConditionalHashBlock> shiftedPossibilities = new ArrayList<ConditionalHashBlock>();
    for (ConditionalHashBlock possibility : possibilities) {
      shiftedPossibilities.add(possibility.shifted(shift));
    }
    return new MultiHashBlock(shiftedPossibilities);
  }

  public String toString(Sequence sequence) {
    StringBuilder builder = new StringBuilder();
    builder.append("(");
    boolean first = true;
    for (ConditionalHashBlock conditional : this.possibilities) {
      if (first) {
        first = false;
      } else {
        builder.append("|");
      }
      builder.append(conditional.toString(sequence)); 
    }
    builder.append(")");
    return builder.toString();
  }   
  private List<ConditionalHashBlock> possibilities;
}
