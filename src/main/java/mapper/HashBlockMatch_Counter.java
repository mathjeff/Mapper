package mapper;

import java.util.List;

public class HashBlockMatch_Counter {
  public HashBlockMatch_Counter(SequenceMatch match, List<HashBlock> matchHistory, int initialNumDistinctMismatches, int lastMismatchedPosition) {
    this.match = match;
    this.matchHistory = matchHistory;
    this.numDistinctMismatches = initialNumDistinctMismatches;
    this.lastMismatchedPosition = lastMismatchedPosition;
    this.historyProcessed_index = matchHistory.size() - 1;
  }

  public int getNumMatches() {
    return numMatches;
  }

  public int getNumDistinctMismatches() {
    this.update();
    return numDistinctMismatches;
  }

  public void addMatch(SequenceMatch match, HashBlock block) {
    // this.match = match;
    numMatches++;
    this.lastMatchedBlock = block;
  }

  public int getLastMismatchedPosition() {
    return this.lastMismatchedPosition;
  }

  public HashBlock getLastMatchedBlock() {
    return this.lastMatchedBlock;
  }

  public SequenceMatch getMatch() {
    return this.match;
  }

  public void update() {
    while (this.historyProcessed_index < this.matchHistory.size()) {
      this.update(matchHistory.get(this.historyProcessed_index));
      this.historyProcessed_index++;
    }
  }

  public void setGood() {
    this.good = true;
    this.priority = this.getNumDistinctMismatches();
  }

  public boolean isGood() {
    return this.good;
  }

  public int getPriority() {
    return this.priority;
  }

  public void setNextCounter(HashBlockMatch_Counter next) {
    this.nextCounter = next;
  }
  public HashBlockMatch_Counter getNextCounter() {
    return this.nextCounter;
  }
  public void setPreviousCounter(HashBlockMatch_Counter prev) {
    this.previousCounter = prev;
  }
  public HashBlockMatch_Counter getPreviousCounter() {
    return this.previousCounter;
  }

  private void update(HashBlock block) {
    // make sure it's a mismatch
    if (block != this.lastMatchedBlock) {
      int blockStart = block.getStartIndex();
      int blockEnd = block.getEndIndex();
      // make sure it's a new mismatch
      if (blockStart >= this.lastMismatchedPosition) {
        // make sure the reference exists here
        if (this.match.getOffset() + blockEnd <= this.match.getSequenceB().getLength()) {
          this.numDistinctMismatches++;
          this.lastMismatchedPosition = blockEnd;
        }
      }
    }
  }

  int numMatches;
  int numDistinctMismatches;
  int lastMismatchedPosition;
  HashBlock lastMatchedBlock;
  SequenceMatch match;

  List<HashBlock> matchHistory;
  int historyProcessed_index;
  boolean good;
  HashBlockMatch_Counter nextCounter;
  HashBlockMatch_Counter previousCounter;


  // Tells how early we want to consider this counter
  // Lower priority is better. 0 is the best.
  // This value equals the number of distinct mismatched hashblocks found when setGood() is called
  int priority;
}
