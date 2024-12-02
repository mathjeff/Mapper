package mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// A HashBlock_ParentRow is a collection of nearby HashBlock's
public class HashBlock_ParentRow implements HashBlock_Row {
  private static int maxNumCombinationsToExpand = 64;
  public HashBlock_ParentRow(HashBlock_Row previousBatch, boolean assumeOnlyUsedOnce, HashBlock_Buffer blockListener) {
    this.previousBatch = previousBatch;
    this.sequence = previousBatch.getSequence();
    this.assumeOnlyUsedOnce = assumeOnlyUsedOnce;
    this.blockListener = blockListener;
    this.maxPositionChecked = -1;
    this.level = previousBatch.getLevel() + 1;
  }

  public IMultiHashBlock get(int index) {
    IMultiHashBlock next = this.getAfter(index - 1);
    if (next != null && next.getStartIndex() == index)
      return next;
    return null;
  }

  public IMultiHashBlock getAfter(int position) {
    // check for previously created blocks
    if (position < this.maxPositionChecked) {
      IMultiHashBlock prev = null;
      for (int i = this.blockList.size() - 1; i >= 0; i--) {
        IMultiHashBlock block = this.blockList.get(i);
        if (block.getStartIndex() > position) {
          prev = block;
        } else {
          break;
        }
      }
      if (prev != null) {
        return prev;
      }
    }
    // see if we can make a new block
    while (true) {
      if (this.maxPositionChecked >= this.sequence.getLength()) {
        // reached the end of the sequence
        break;
      }
      if (this.blockList.size() > 0) {
        IMultiHashBlock lastBlock = this.blockList.get(this.blockList.size() - 1);
        if (lastBlock.getStartIndex() > position) {
          // found the next block after this position
          return lastBlock;
        }
      }
      this.maybeMakeBlock();
    }

    return null;
  }

  public void skipTo(int index) {
    if (this.maxPositionChecked < index && this.assumeOnlyUsedOnce) {
      this.maxPositionChecked = index;
      this.blockList.clear();
    }
  }

  private void maybeMakeBlock() {
    // set up some variables
    int afterIndex = this.maxPositionChecked;
    IMultiHashBlock leftBlock = this.previousBatch.getAfter(afterIndex);
    //System.err.println("maybeMakeBlock index " + afterIndex);
    if (leftBlock == null) {
      // no more blocks to create
      this.maxPositionChecked = this.sequence.getLength();
      return;
    }
    int index = leftBlock.getStartIndex();
    this.maxPositionChecked = index;
    int afterIndex2;
    HashBlock leftSingle = leftBlock.getSingle();
    afterIndex2 = index;
    IMultiHashBlock rightBlock = previousBatch.getAfter(afterIndex2);

    // make sure we have another hashblock to merge with
    if (rightBlock != null) {
      // Check whether either IMultiHashBlock is more than just one HashBlock
      leftSingle = leftBlock.getSingle();
      HashBlock rightSingle = rightBlock.getSingle();
      if (leftSingle != null && rightSingle != null) {
        // We have two concrete HashBlocks, we can simply merge them
        HashBlock merged = this.maybeMergeBlocks(leftSingle, rightSingle);
        if (merged != null) {
          this.putBlock(merged);
        }
      } else {
        // We have one or more MultiHashBlock. We have to merge multiple combinations
        List<ConditionalHashBlock> mergeOptions = new ArrayList<ConditionalHashBlock>();
        for (ConditionalHashBlock leftOption : leftBlock.getPossibilities()) {
          leftSingle = leftOption.getHashBlock();
          if (leftSingle != null) {
            this.expand(leftSingle, leftOption.getCondition(), index, mergeOptions);
          } else {
            // If the left hashblock doesn't exist, then the merge doesn't exist either
            mergeOptions.add(new ConditionalHashBlock(null, leftOption.getCondition()));
          }
        }
        if (mergeOptions.size() > 0 && mergeOptions.size() <= maxNumCombinationsToExpand) {
          // If we found a possible option, add it
          // If we only discovered things that can't exist, then we don't need to save any of them
          boolean hasNonEmpty = false;
          for (ConditionalHashBlock mergeCandidate : mergeOptions) {
            if (mergeCandidate.getHashBlock() != null)
              hasNonEmpty = true;
          }
          if (hasNonEmpty)
            this.putBlock(new MultiHashBlock(mergeOptions));
        }
      }
    }

    // next time make sure to start at the next block
    if (this.assumeOnlyUsedOnce) {
      this.previousBatch.garbageCollect(index);
    }
  }

  private void putBlock(IMultiHashBlock block) {
    this.blockList.add(block);
    if (this.blockListener != null) {
      blockListener.addHashblock(block);
    }
  }

  // Given a ConditionalHashBlock in the parent row, computes the possible hash blocks we might build on top of it
  private void expand(HashBlock leftBlock, SequenceCondition startingCondition, int startIndex, List<ConditionalHashBlock> results) {
    if (leftBlock == null)
      throw new IllegalArgumentException("Cannot extend null block");
    IMultiHashBlock next = this.previousBatch.getAfter(startIndex);
    if (next == null) {
      //System.err.println("No parent blocks remain, expansion done");
      // No HashBlocks remain to merge with
      return;
    }
    boolean foundAnIntersection = false;
    for (ConditionalHashBlock rightOption : next.getPossibilities()) {
      SequenceCondition intersectionCondition = startingCondition.intersect(rightOption.getCondition());
      if (intersectionCondition == null) {
        // It's not possible for this block to exist at the same time as the previous
        // If the previous hashblock merges with an alternate hashblock, it will be covered by another case
        //System.err.println("Other condition " + rightOption.getCondition() + " conflicts with this condition " + startingCondition);

        if (foundAnIntersection) {
          // Conditions are sorted, with earlier positions being sorting keys than later positions
          // So, once we find one intersecting condition and then we find a nonintersecting block, there won't be any more intersecting blocks
          break;
        }
        continue;
      }
      foundAnIntersection = true;
      // Expanding more ambiguities allows us to detect more alignments but also takes more time
      // We would like to allow the expansion of up to 3 positions each having 4 possibilities (N), or an equivalent number of positions each having 2 possibilities
      // So, we express this limit in terms of the number of possibilities
      if (results.size() > maxNumCombinationsToExpand) {
        // If we expand all combinations of ConditionalHashBlock, that could potentially take a while
        // So, we ignore creating new ConditionalHashBlocks past a certain complexity
        return;
      }

      HashBlock rightBlock = rightOption.getHashBlock();
      if (rightBlock == null) {
        // Under this set of conditions, no hashblock exists here
        // Check the next position
        //System.err.println("Recursing again for rightOption condition " + rightOption.getCondition());
        this.expand(leftBlock, intersectionCondition, next.getStartIndex(), results);
        continue;
      }

      HashBlock merged = this.maybeMergeBlocks(leftBlock, rightBlock);
      if (merged == null) {
        // These hash blocks don't want to merge
        // There will probably be another pair of parent hashblocks that do want to merge, though
        // So, we record the fact that this pair doesn't want to merge by explicitly adding <null>
        //System.err.println("Considered merging " + leftBlock.toString(this.sequence) + " and " + rightBlock.toString(this.sequence) + ", didn't merge");
        results.add(new ConditionalHashBlock(null, intersectionCondition));
      } else {
        results.add(new ConditionalHashBlock(merged, intersectionCondition));
      }
    }
  }

  private HashBlock maybeMergeBlocks(HashBlock left, HashBlock right) {
    if (shouldMergeBlocks(left, right)) {
      return this.mergeBlocks(left, right);
    }
    return null;
  }

  private boolean shouldMergeBlocks(HashBlock left, HashBlock right) {
    if (left.getEndIndex() < right.getStartIndex())
      return false; // a block inbetween was removed due to too much ambiguity
    if (left.get_requestMergeRight())
      return true;
    if (right.get_requestMergeLeft())
      return true;
    return false;
  }

  // combines two HashBlock's into one HashBlock
  private HashBlock mergeBlocks(HashBlock left, HashBlock right) {
    int startIndex = left.getStartIndex();
    int endIndex = right.getEndIndex();
    return new HashBlock(this.sequence, startIndex, endIndex - startIndex, left, right);
  }

  public Sequence getSequence() {
    return this.sequence;
  }

  public void garbageCollect(int index) {
    for (int i = 0; i < this.blockList.size(); i++) {
      if (this.blockList.get(i).getStartIndex() == index) {
        this.blockList.remove(i);
        return;
      }
    }
  }

  public int getLevel() {
    return this.level;
  }

  private Sequence sequence;
  //private Map<Integer, IMultiHashBlock> blocks = new HashMap<Integer, IMultiHashBlock>();
  private HashBlock_Row previousBatch;
  private int maxPositionChecked;
  private int count;
  private boolean assumeOnlyUsedOnce;
  private HashBlock_Buffer blockListener;
  private List<IMultiHashBlock> blockList = new ArrayList<IMultiHashBlock>();
  private int level;
}
