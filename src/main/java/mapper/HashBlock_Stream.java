package mapper;

import java.util.ArrayList;
import java.util.List;

// A HashBlock_Stream is essentially a lazy List<List<HashBlock>>.
// A HashBlock_Stream produces collections of HashBlock, one at a time.
// It does this by starting with lots of individual HashBlocks and gradually combining them with their
// neighbors until there are none left.
// The way in which HashBlocks are combined in deterministic based on the contents of those blocks.
// So, when given the same substring in various positions, the pattern of blocks produced for that
// substring will always be the same, to facilitate matching via hashing.
// Additionally, the number of HashBlocks with any given size decays exponentially with respect to
// their size, so the total number of HashBlocks produced for a given sequence is a constant times the
// length of that sequence.
public class HashBlock_Stream {
  public HashBlock_Stream(Sequence sequence, boolean assumeOnlyUsedOnce, HashBlock_Buffer blockListener) {
    this.blocks = new HashBlock_BaseRow(sequence, blockListener);
    this.sequence = sequence;
    this.assumeOnlyUsedOnce = assumeOnlyUsedOnce;
    this.blockListener = blockListener;
  }

  // combines the current batch of blocks into a new batch, and returns the previous batch
  public HashBlock_Row getNextBatch() {
    this.ensureFreshBlocks();
    this.emittedCurrentBlocks = true;
    return this.blocks;
  }

  // ensures that this.blocks refers to a batch that we haven't yet given to the user
  private void ensureFreshBlocks() {
    if (this.emittedCurrentBlocks) {
      this.advance();
    }
  }

  // advances this.blocks to the next batch
  private void advance() {
    this.blocks = new HashBlock_ParentRow(this.blocks, this.assumeOnlyUsedOnce, this.blockListener);
    // Check whether we can use the HashBlock_Compiler
    // The HashBlock_Compiler can skip generating some hashblocks for parent rows, so if we are listening for all parent hashblocks then we can't use the compiler
    boolean blockListenerAllowsCompiler;
    if (this.blockListener == null) {
      // if there is no block listener then we don't need to worry about generating all hashblocks for parent rows
      blockListenerAllowsCompiler = true;
    } else {
      int maxBlockLength = ((int)Math.pow(2, this.blocks.getLevel()));
      int maxGapmerLength = HashBlock.getMaxGapmerNumBasepairsUsed(maxBlockLength);
      if (this.blockListener.getMinInterestingSize() > maxGapmerLength) {
        // if this.blockListener is not interested in blocks from this row, then it's ok to use a compiler
        blockListenerAllowsCompiler = true;
      } else {
        // block listener might be interested in blocks from parent rows
        blockListenerAllowsCompiler = false;
      }
      blockListenerAllowsCompiler = false;
    }
    if (blockListenerAllowsCompiler) {
      // Check whether it is more efficient to use the HashBlock_Compiler
      // For rows at higher levels, the number of combinations to save is too many for our compiler to be faster
      // For the first row, the existing logic is already as fast as a compiler
      // For rows just above the bottom row, the compiler should help a little bit
      if (this.blocks.getLevel() <= 3 && this.blocks.getLevel() > 0) {
        this.blocks = new HashBlock_Compiler(this.blocks);
      }
    }
    this.emittedCurrentBlocks = false;
  }

  private Sequence sequence;
  private HashBlock_Row blocks;
  // whether this.blocks has been given to a caller already
  private boolean emittedCurrentBlocks = false;
  private boolean assumeOnlyUsedOnce;
  private HashBlock_Buffer blockListener;
}
