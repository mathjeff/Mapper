package mapper;

import java.util.ArrayList;
import java.util.List;

// A HashBlock represents a portion of a genome sequence
// A HashBlock is used by a HashBlock_Sequence for comparing multiple sequences
// A HashBlock represents the part of the HashBlock that we use when building it
public class HashBlock implements IMultiHashBlock {

  public static int getMaxGapmerNumBasepairsUsed(int startingLength) {
    return startingLength + startingLength * 9 / 8 + 1;
  }

  public static int getMaxGapmerLength(int startingLength) {
    return startingLength + startingLength * 9 / 4 + 1;
  }

  // creates a HashBlock at a certain position based merging two parent HashBlocks
  public HashBlock(Sequence sequence, int startIndex, int length, HashBlock leftParent, HashBlock rightParent) {
    this.startIndex = startIndex;
    this.length = length;
    this.numBasepairsUsed = length;
    this.mergeHashes(leftParent, rightParent);

    // choose direction of gap
    if (this.requestMergeLeft != this.requestMergeRight) {
      // If the next row will be building a larger hashblock to our left, then it helps to extend our gap to the right for more variety, and vice versa
      if (this.requestMergeLeft)
        this.gapDirection = 1;
      else
        this.gapDirection = -1;
    } else {
      if (leftParent.forwardHash != rightParent.reverseHash) {
        if (leftParent.forwardHash > rightParent.reverseHash)
          this.gapDirection = 1;
        else
          this.gapDirection = -1;
      }
    }
    // It's not very useful to have hashblocks or gapmers that are similar in size to their parents
    // So, if a hashblock is close to its parents' sizes, we adjust the size of its gapmer
    this.extraGapmerLength = (leftParent.getLength() + rightParent.getLength() - this.getLength()) / 4;
  }

  public HashBlock(int startIndex, int length) {
    this.startIndex = startIndex;
    this.length = length;
    this.numBasepairsUsed = length;
  }

  public HashBlock(int startIndex, int length, int forwardHash, int reverseHash) {
    this.startIndex = startIndex;
    this.length = length;
    this.numBasepairsUsed = length;
    this.forwardHash = forwardHash;
    this.reverseHash = reverseHash;
  }

  public HashBlock(char itemHere, int index) {
    this.startIndex = index;
    this.length = 1;
    this.numBasepairsUsed = this.length;
    this.hashChar(itemHere);
  }

  public HashBlock withGapAndExtension(Sequence sequence) {
    // We to build HashBlocks such that it should be likely that at least a couple of them will uniquely identify the correct position in the reference genome
    // This requires a HashBlock that is long enough and doesn't overlap any mutations
    // Suppose that a unique match requires about B basepairs
    // Suppose that there is an average of 1 mutation for every B basepairs
    // If we created a shape that was just B contiguous basepairs, it's possible that every one would overlap a mutation
    // Instead we try to create a shape consisting of:
    // One subblock of length B*2/3, where its basepairs are included in the hashcode
    // One gap of length B/3
    // One subblock of length B/3, where its basepairs are also included in the hashcode
    // This creates a shape like this: XX_X
    // If adjacent mutations are within a distance of B/3, they can both fall outside our block
    // If adjacent mutations are a distance of B/3 to B*2/3 apart, then one can fall inside the gap and the other can be to the right of the block
    // If adjacent mutations are a distance of B*2/3 to B basepairs apart, then one can fall inside the gap and the other can be to the left of the block
    //
    // We use our current hashblock as the left side of this gapped block ("XX" in "XX_X")
    // We want the gap to be roughly half the length of the current block
    // We want the subsequent extension to be roughly half the length of the current block
    // We want these lengths to be slightly different across hashblocks so the HashBlock_Database can use the length to help separate HashBlocks into groups
    //
    // So, we compute pseudorandom lengths close to this.length / 2 like this:

    int extensionHash = 0;

    int targetExtraLength;
    int gapLength;
    int extensionLength;

    // We want the Gapped_HashBlock to be about twice as long as this hashblock
    targetExtraLength = this.length;
    // We don't want to only generate even-length or odd-length Gapped_HashBlocks so we adjust the length more
    targetExtraLength += Math.abs(Math.max(this.forwardHash, this.reverseHash)) % 3;
    targetExtraLength += this.extraGapmerLength;
    gapLength = this.length / 2;
    extensionLength = targetExtraLength - gapLength;

    if (gapDirection == 0)
      return this;

    boolean leftGap = (gapDirection < 0);
    HashBlock result;
    if (leftGap) {
      int extensionEnd = this.getStartIndex() - gapLength;
      int extensionStart = extensionEnd - extensionLength;
      if (extensionStart < 0) {
        return null; // no space to extend
      }
      for (int i = extensionEnd - 1; i >= extensionStart; i--) {
        extensionHash *= 7654337;
        char c = sequence.charAt(i);
        extensionHash += charToInt(c);
        /*if (Basepairs.isAmbiguous(b)) {
          return null; // not supported right now, but should be harmless and it's faster not to check for it
        }*/
      }
      result = new Gapped_HashBlock(extensionStart, extensionLength, gapLength, this.length);
    } else {
      // rightGap
      int extensionStart = this.getEndIndex() + gapLength;
      int extensionEnd = extensionStart + extensionLength;
      if (extensionEnd > sequence.getLength()) {
        return null; // no space to extend
      }
      for (int i = extensionStart; i < extensionEnd; i++) {
        extensionHash *= 7654337;
        char c = Basepairs.decode(Basepairs.complement(sequence.encodedCharAt(i)));
        extensionHash += charToInt(c);
        /*if (Basepairs.isAmbiguous(c)) {
          return null; // not supported right now, but should be harmless and it's faster not to check for it
        }*/
      }
      result = new Gapped_HashBlock(this.getStartIndex(), this.length, gapLength, extensionLength);
    }
    result.forwardHash = this.forwardHash + extensionHash;
    result.reverseHash = this.reverseHash + extensionHash;
    result.numBasepairsUsed = this.length + extensionLength;
    if (result.getNumBasepairsUsed() > getMaxGapmerNumBasepairsUsed(this.length)) {
      throw new IllegalArgumentException("Internal error: hashblock of length " + this.length + " expanded into gapmer with number of basepairs used " + result.getNumBasepairsUsed() + " which is more than expected " + getMaxGapmerNumBasepairsUsed(this.length));
    }
    if (result.getLength() > getMaxGapmerLength(this.length)) {
      throw new IllegalArgumentException("Internal error: hashblock of length " + this.length + " expanded into gapmer of length " + result.getLength() + " which is more than expected " + getMaxGapmerLength(this.length));
    }
    return result;
  }

  private int charToInt(char c) {
    if (c == 'A') {
      return 1;
    } else {
      if (c == 'C') {
        return 2;
      } else {
        if (c == 'G') {
          return 3;
        } else {
          if (c == 'T') {
            return 4;
          }
        }
      }
    }
    return 0;
  }

  private void hashChar(char itemHere) {
    if ('A' == itemHere) {
      this.forwardHash = 0;
    } else if ('C' == itemHere) {
      this.forwardHash = 1;
    } else if ('G' == itemHere) {
      this.forwardHash = 2;
    } else { // T
      this.forwardHash = 3;
    }
    if (this.forwardHash / 2 == 0)
      this.requestMergeLeft = true;
    this.requestMergeRight = !this.requestMergeLeft;
    if (this.forwardHash % 2 == 0)
      this.nextRequestMergeLeft = true;
    this.nextRequestMergeRight = !this.nextRequestMergeLeft;
    this.reverseHash = 3 - this.forwardHash;
  }

  // Computes some hashes of the content of this block
  // contentHash is an arbitrary number for helping determine whether this block has the same content as another.
  private void mergeHashes(HashBlock leftParent, HashBlock rightParent) {
    this.forwardHash = mergeHashes(leftParent.length, leftParent.forwardHash, rightParent.length, rightParent.forwardHash);
    this.reverseHash = mergeHashes(rightParent.length, rightParent.reverseHash, leftParent.length, leftParent.reverseHash);

    this.requestMergeLeft = this.requestMergeRight = true;
    this.nextRequestMergeLeft = this.nextRequestMergeRight = true;
    HashBlock anchorParent = null;
    HashBlock otherParent = null;
    if (leftParent.forwardHash != rightParent.reverseHash) {
      if (leftParent.forwardHash > rightParent.reverseHash) {
        anchorParent = rightParent;
        otherParent = leftParent;
      } else {
        anchorParent = leftParent;
        otherParent = rightParent;
      }
    }

    if (anchorParent != null) {
      if (this.forwardHash != this.reverseHash) {
        boolean isReverse = this.forwardHash < this.reverseHash;
        // When the anchor parent moves from left to right we want to invert the merge direction
        // When we move to the reverse complement sequence, we also want to invert the merge direction
        boolean invert = isReverse == (anchorParent == rightParent);

        boolean anchorNextLeft = anchorParent.nextRequestMergeLeft;
        boolean anchorNextRight = anchorParent.nextRequestMergeRight;
        if (anchorNextLeft && anchorNextRight) {
          if (anchorParent == rightParent)
            anchorNextRight = false;
          else
            anchorNextLeft = false;
        }

        boolean otherNextLeft = otherParent.nextRequestMergeLeft;
        boolean otherNextRight = otherParent.nextRequestMergeRight;
        if (otherNextLeft && otherNextRight) {
          if (otherParent == rightParent)
            otherNextLeft = false;
          else
            otherNextRight = false;
        }

        this.requestMergeLeft = anchorNextLeft != invert;
        this.requestMergeRight = anchorNextRight != invert;
        this.nextRequestMergeLeft = otherNextLeft != invert;
        this.nextRequestMergeRight = otherNextRight != invert;
      }
    }

    if (leftParent.length != rightParent.length) {
      this.requestMergeLeft = (leftParent.length > rightParent.length);
      this.requestMergeRight = !this.requestMergeLeft;
      this.nextRequestMergeLeft = !this.requestMergeLeft;
      this.nextRequestMergeRight = !this.nextRequestMergeLeft;
    }

    if (this.forwardHash != this.reverseHash) {
      if (this.requestMergeLeft && this.requestMergeRight) {
        this.requestMergeLeft = (this.forwardHash > this.reverseHash);
        this.requestMergeRight = !this.requestMergeLeft;
      }
      if (this.nextRequestMergeLeft && this.nextRequestMergeRight) {
        this.nextRequestMergeLeft = this.requestMergeLeft;
        this.nextRequestMergeRight = !this.nextRequestMergeLeft;
      }
    }
  }

  private int mergeHashes(int leftLength, int leftContentHash, int rightLength, int rightContentHash) {
    long rotatedLeft = ((long)leftContentHash + 1) * (54323 + 323 * (long)rightLength);
    long rotatedRight = (long)((long)(rightContentHash + 1) * (long)(leftLength));
    long longTopBits = rotatedLeft + rotatedRight;
    int topBits = (int)longTopBits + (int)(longTopBits >> 32);

    // we allow and expect contentHash to overflow
    return topBits;
  }

  public String getText(Sequence sequence) {
    return sequence.getRange(this.startIndex, this.length);
  }

  public String getTextAt(Sequence sequence, int startIndex) {
    return sequence.getRange(startIndex, this.length);
  }

  // Whether this block wants to merge with its left neighbor next.
  public boolean get_requestMergeLeft() {
    return this.requestMergeLeft;
  }

  public boolean get_requestMergeRight() {
    return this.requestMergeRight;
  }

  public boolean get_nextRequestMergeLeft() {
    return this.nextRequestMergeLeft;
  }

  public boolean get_nextRequestMergeRight() {
    return this.nextRequestMergeRight;
  }

  public int getStartIndex() {
    return this.startIndex;
  }

  public int getEndIndex() {
    return this.startIndex + length;
  }

  public int getLength() {
    return this.length;
  }

  public int getNumBasepairsUsed() {
    return this.numBasepairsUsed;
  }

  // the hash of the content of this hashblock
  public int getForwardHash() {
    return this.forwardHash;
  }

  // the hash of the reverse complement of this hashblock
  public int getReverseHash() {
    return this.reverseHash;
  }

  public int getLookupKey() {
    if (this.isPrimaryPolarity())
      return this.getForwardHash();
    return this.getReverseHash();
  }

  // whether this hashblock (rather than its reverse complement) is worth saving in a hash table
  public boolean isPrimaryPolarity() {
    // Prefer to save hashblocks having extensions going to the left so that almost all saved hashblocks are oriented in the same direction
    if (this.requestMergeLeft != this.requestMergeRight)
      return this.requestMergeLeft;
    return this.forwardHash >= this.reverseHash;
  }

  public boolean isSecondaryPolarity() {
    return this.forwardHash <= this.reverseHash;
  }

  public SequencePosition toMatchView(Sequence sequence) {
    return new SequencePosition(sequence, this.getStartIndex());
  }

  // used by IMultiHashBlock
  public HashBlock getSingle() {
    return this;
  }

  // used by IMultiHashBlock
  public List<ConditionalHashBlock> getPossibilities() {
    List<ConditionalHashBlock> thisList = new ArrayList<ConditionalHashBlock>(1);
    thisList.add(new ConditionalHashBlock(this, SequenceCondition.ALWAYS));
    return thisList;
  }
  public int getMinLength() {
    return this.getLength();
  }

  public String toString(Sequence sequence) {
    return sequence.getName() + "[" + this.getStartIndex() + ":" + this.getEndIndex() + "] = " + this.getText(sequence) + ", hash= " + this.getForwardHash();
  }

  public IMultiHashBlock withEnd(int index) {
    return shifted(index - this.getEndIndex());
  }

  public IMultiHashBlock shifted(int shift) {
    if (shift == 0)
      return this;
    HashBlock result = new HashBlock(startIndex + shift, length);
    result.numBasepairsUsed = this.numBasepairsUsed;
    result.forwardHash = this.forwardHash;
    result.reverseHash = this.reverseHash;
    result.gapDirection = this.gapDirection;
    result.requestMergeLeft = this.requestMergeLeft;
    result.requestMergeRight = this.requestMergeRight;
    result.nextRequestMergeLeft = this.nextRequestMergeLeft;
    result.nextRequestMergeRight = this.nextRequestMergeRight;
    result.extraGapmerLength = this.extraGapmerLength;
    return result;
  }

  private int startIndex;
  private int length;
  private int numBasepairsUsed;
  private int forwardHash;
  private int reverseHash;

  private int gapDirection;
  private int extraGapmerLength;

  private boolean requestMergeLeft;
  private boolean requestMergeRight;
  private boolean nextRequestMergeLeft;
  private boolean nextRequestMergeRight;

}
