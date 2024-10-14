package mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// a HashBlockPath is an interesting path through a HashBlock_Stream
// A HashBlock_Database determines what's interesting. We want to have a small but nonzero number of matches at each step in the path
public class HashBlockPath {
  static long[] emptyList = new long[0];

  public HashBlockPath(HashBlock_Pyramid pyramid, Readable_HashBlock_Database database, SequenceDatabase sequenceDatabase, Sequence query, Logger logger, String queryShortName) {
    this.pyramid = pyramid;
    this.database = database;
    this.sequenceDatabase = sequenceDatabase;
    this.query = query;
    this.currentBlock = this.getStartingBlock(0);
    this.logger = logger;
    this.queryShortName = queryShortName;
    this.skipMultiblocks();
  }

  // steps to the next interesting HashBlock_Match in the path
  public HashBlock getNextInterestingBlock() {
    if (this.currentBlock == null)
      return null;
    HashBlock result = null;
    while (true) {
      result = this.getNextBlockWithGoodNumberOfMatches();
      if (result == null)
        return null;
      // skip duplication
      if (this.recentlySeen(result)) {
        if (logger.getEnabled()) {
          logger.log("Skipping block " + result.toString(this.query) + " matching previous recent block");
        }
        continue;
      }
      // skip high overlap
      if (this.previousBlock != null && this.previousBlock.getStartIndex() + this.previousBlock.getLength() / 4 >= result.getStartIndex()) {
        logger.log("Skipping block " + result.toString(this.query) + " with significant overlap with previous block " + this.previousBlock.toString(this.query));
        continue;
      }
     break;
    }
    return result;
  }

  private boolean recentlySeen(HashBlock block) {
    // check for hashblocks that we've just recently seen
    boolean result = false;
    if (this.previousInterestingBlock != null && block.getForwardHash() == previousInterestingBlock.getForwardHash()) {
      result = true;
    } else {
      if (this.previousPreviousInterestingBlock != null && block.getForwardHash() == previousPreviousInterestingBlock.getForwardHash()) {
        result = true;
      }
    }
    this.previousPreviousInterestingBlock = this.previousInterestingBlock;
    this.previousInterestingBlock = block;
    return result;
  }

  // steps to the next HashBlock in the path having a sufficiently small number of matches
  private HashBlock getNextBlockWithGoodNumberOfMatches() {
    HashBlock previous = this.currentBlock.getSingle();
    // advance until we find an interesting block, and return it
    while (true) {
      HashBlock next = this.advanceToNextPosition();
      if (next == null)
        return null;
      HashBlock extended = this.withGap();
      if (extended == null) {
        if (this.logger.getEnabled()) {
          this.logger.log(this.queryShortName + "[" + next.getStartIndex() + ":" + next.getEndIndex() + "] =  " + spaces(next.getStartIndex()) + next.getText(this.query) + " cannot expand into gapmer");
        }
        continue;
      }
      if (!this.hasFewEnoughMatches(extended)) {
        if (this.logger.getEnabled()) {
          HashBlock block = extended;
          int numMatches = this.database.getNumMatches(block);
          String numMatchesText;
          if (numMatches == Integer.MAX_VALUE)
            numMatchesText = "uncounted";
          else
            numMatchesText = "" + numMatches;
          logger.log(this.queryShortName + "[" + block.getStartIndex() + ":" + block.getEndIndex() + "] =  " + spaces(block.getStartIndex()) + block.getText(this.query) + " reports too many matches: " + numMatchesText + " > " + this.getMaxNumMatchesAllowed(block));
        }
        continue; // this block matched too many things; keep searching
      }
      return extended; // we know all the places where this block matches
    }
  }

  // Moves down to a smaller HashBlock
  private void moveDown() {
    this.batchIndex--;
    HashBlock currentSingle = this.currentBlock.getSingle();
    if (currentSingle != null) {
      this.currentBlock = this.pyramid.get(this.batchIndex).getAfter(currentSingle.getStartIndex());
    } else {
      this.currentBlock = this.pyramid.get(this.batchIndex).getAfter(this.currentBlock.getStartIndex());
    }
    this.currentGapmer = null;
  }

  // Attemps to move upward and right, unless that would produce a nonoverlapping hashblock, in which case just moves right
  private void moveUpOrRight() {
    // move right
    HashBlock left = this.currentBlock.getSingle();
    IMultiHashBlock up = this.pyramid.get(this.batchIndex + 1).get(left.getStartIndex());
    if (up != null && up.getStartIndex() <= left.getStartIndex()) {
      this.batchIndex++;
      this.currentBlock = up;
      this.currentGapmer = null;
    } else {
      this.moveRight();
    }
  }

  // moves to the next HashBlock in the current row
  private void moveRight() {
    this.currentBlock = this.pyramid.get(this.batchIndex).getAfter(this.currentBlock.getStartIndex());
    this.currentGapmer = null;
  }

  private void skipMultiblocks() {
    while (true) {
      if (this.currentBlock == null || this.currentBlock.getSingle() != null)
        return;
      if (this.batchIndex > 0) {
        this.moveDown();
      } else {
        this.moveRight();
      }
    }
  }

  // steps to the next HashBlock_Match in the path
  private HashBlock advanceToNextPosition() {
    HashBlock single = this.currentBlock.getSingle();
    if (HashBlock.getMaxGapmerNumBasepairsUsed(single.getLength()) < this.database.getMinInterestingSize() && this.database.getEnableGapmers()) {
      // If this block is too short, it will have too many matches and we can just skip to trying to make it bigger
      this.moveUpOrRight();
    } else {
      HashBlock extended = this.withGap();
      if (extended != null) {
        int numMatches = this.database.getNumMatches(extended);

        // We would like to have few enough matches for the search to be fast
        // Each subsequent level reduces the number of hashblocks to about 3/4
        //  Therefore, if the sequences are random, each subsequent level should reduce the number of matches of a given hashblock to about 3/4
        // Computing each subsequent level requires approximately 1 additional unit of work per hashblock
        // The sum of these two costs should be approximately minimized when there are 4 matches
        // However, allowing more matches is probably more accurate, so we increase this number a little bit
        // So, if we get a smaller number of matches than that, then we shrink the hashblock to get more matches
        if (numMatches < 6) {
          if (this.batchIndex > 0) {
            this.moveDown();
          } else {
            this.moveRight();
          }
        } else {
          if (numMatches > getMaxNumMatchesAllowed(extended)) {
            // If we have so many matches that we don't have time to check them, then we make the hashblock larger so we can make use of this part of the query sequence
            this.moveUpOrRight();
          } else {
            // If we don't have too many or too few matches, we can just move to the next hashblock to the right, which is faster than repeatedly growing and shrinking the hashblock size
            this.moveRight();
          }
        }
      } else {
        int typicalGapmerNumBasepairsUsed = single.getLength() * 3 / 2;
        if (typicalGapmerNumBasepairsUsed <= this.database.getMinInterestingSize() && this.database.getEnableGapmers()) {
          // This block is probably too small so we try a larger block
          this.moveUpOrRight();
        } else {
          // This block might be too large so we try a smaller block
          if (this.batchIndex > 0) {
            this.moveDown();
          } else {
            this.moveRight();
          }
        }
      }
    }
    this.skipMultiblocks();
    if (this.currentBlock == null)
      return null;
    // this.currentBlock.getSingle() won't be null because we will have called skipMultiblocks
    return this.currentBlock.getSingle();
  }

  private HashBlock withGap() {
    if (!this.database.getEnableGapmers())
      return this.currentBlock.getSingle();
    if (this.currentGapmer == null)
      this.currentGapmer = this.currentBlock.getSingle().withGapAndExtension(this.query);
    return this.currentGapmer;
  }

  private int getMaxNumMatchesAllowed(HashBlock block) {
    // For hashblocks that use a small fraction of the query, we can probably just lengthen the hashblock and try again
    // If a hashblock can't be lengthed much more, we allow checking more matches instead
    if (block.getLength() >= this.query.getLength() / 5) {
      return this.database.getMaxNumMatchesAllowed(block);
    }
    return block.getNumBasepairsUsed();
  }

  private boolean hasFewEnoughMatches(HashBlock block) {
    return database.getNumMatches(block) <= getMaxNumMatchesAllowed(block);
  }

  private IMultiHashBlock getStartingBlock(int level) {
    HashBlock_Row batch = this.pyramid.get(level);
    return batch.getAfter(-1);
  }

  private String spaces(int count) {
    String result = "";
    for (int i = 0; i < count; i++) {
      result += " ";
    }
    return result;
  }

  int batchIndex;
  IMultiHashBlock currentBlock;
  HashBlock currentGapmer;
  HashBlock previousBlock;
  HashBlock previousInterestingBlock;
  HashBlock previousPreviousInterestingBlock;
  HashBlock_Pyramid pyramid;
  Readable_HashBlock_Database database;
  SequenceDatabase sequenceDatabase;
  Sequence query;
  int maxStartVisited;
  Logger logger;
  String queryShortName;

  boolean done = false;

  int numLookupResults;
}
