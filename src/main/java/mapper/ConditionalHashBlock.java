package mapper;

import java.util.List;

// A ConditionalHashBlock says that a HashBlock exists at a certain location if a certain condition is met
// A ConditionalHashBlock gets created from an ambiguous sequence (containing, for example, 'N')
public class ConditionalHashBlock {
  public ConditionalHashBlock(HashBlock hashBlock, SequenceCondition condition) {
    this.hashBlock = hashBlock;
    this.condition = condition;
    /*if (this.hashBlock == null) {
      throw new IllegalArgumentException("null hashblock for " + this.condition.toString());
    }*/
  }

  public HashBlock getHashBlock() {
    return this.hashBlock;
  }

  public SequenceCondition getCondition() {
    return this.condition;
  }

  public ConditionalHashBlock shifted(int shift) {
    if (shift == 0)
      return this;
    HashBlock shiftedBlock = null;
    if (hashBlock != null) {
      shiftedBlock = (HashBlock)hashBlock.withEnd(hashBlock.getEndIndex() + shift);
    }

    SequenceCondition shiftedCondition = condition.shifted(shift);
    return new ConditionalHashBlock(shiftedBlock, shiftedCondition);
  }

  public String toString(Sequence sequence) {
    String hashBlockText = null;
    if (this.hashBlock != null)
      hashBlockText = this.hashBlock.toString(sequence);
    return "(" + hashBlockText + " with " + this.condition.toString() + ")";
  }

  private HashBlock hashBlock;
  private SequenceCondition condition;
}
