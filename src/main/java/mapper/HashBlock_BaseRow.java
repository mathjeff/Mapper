package mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashBlock_BaseRow implements HashBlock_Row {
  public HashBlock_BaseRow(Sequence sequence, HashBlock_Buffer blockListener) {
    this.sequence = sequence;
    this.blockListener = blockListener;
  }

  private static byte[] encodedChars;
  private static byte[] getEncodedChars() {
    if (encodedChars == null) {
      byte[] newChars = new byte[4];
      newChars[0] = Basepairs.encode('A');
      newChars[1] = Basepairs.encode('C');
      newChars[2] = Basepairs.encode('G');
      newChars[3] = Basepairs.encode('T');
      encodedChars = newChars;
    }
    return encodedChars;
  }

  public IMultiHashBlock get(int index) {
    if (index >= this.sequence.getLength())
      return null;
    IMultiHashBlock block = this.blocks.get(index);
    if (block == null) {
      byte encodedItemHere = this.sequence.encodedCharAt(index);
      if (Basepairs.isAmbiguous(encodedItemHere)) {
        List<ConditionalHashBlock> possibleBlocks = new ArrayList<ConditionalHashBlock>(4);
        for (byte encodedOptionHere : getEncodedChars()) {
          if (Basepairs.canMatch(encodedItemHere, encodedOptionHere)) {
            char optionHere = Basepairs.decode(encodedOptionHere);
            HashBlock possibleBlock = new HashBlock(optionHere, index);
            SequenceCondition condition = new SequenceCondition(index, optionHere);
            possibleBlocks.add(new ConditionalHashBlock(possibleBlock, condition));
          }
        }
        block = new MultiHashBlock(possibleBlocks);
      } else {
        // This sequence knows which character exists here
        char itemHere = Basepairs.decode(encodedItemHere);
        block = new HashBlock(itemHere, index);
      }
      if (block != null) {
        if (this.blockListener != null)
          this.blockListener.addHashblock(block);
        this.blocks.put(index, block);
      }
    }
    return block;
  }

  public void skipTo(int index) {
  }

  private int charToIndex(char c) {
    if (c == 'A')
      return 0;
    if (c == 'C')
      return 1;
    if (c == 'G')
      return 2;
    return 3;
  }

  public IMultiHashBlock getAfter(int index) {
    return get(index + 1);
  }

  public Sequence getSequence() {
    return this.sequence;
  }

  public void garbageCollect(int index) {
    this.blocks.remove(index);
  }

  public int getLevel() {
    return 0;
  }

  private Sequence sequence;
  private HashBlock_Buffer blockListener;
  private Map<Integer, IMultiHashBlock> blocks = new HashMap<Integer, IMultiHashBlock>();
}
