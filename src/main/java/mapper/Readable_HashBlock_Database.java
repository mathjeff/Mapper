package mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// A Readable_HashBlock_Database is a view of a HashBlock_Database
// Each Readable_HashBlock_Database can be accessed in a separate thread
public class Readable_HashBlock_Database {
  private static SequencePosition[] emptyList = new SequencePosition[0];

  public Readable_HashBlock_Database(HashBlock_Database database) {
    this.database = database;
    this.minInterestingSize = database.getMinInterestingSize();
  }

  // returns an array of SequencePosition, each encoded as a long
  public SequencePosition[] matchBlock(HashBlock block) {
    return this.matchBlock(block, Integer.MAX_VALUE);
  }
  public SequencePosition[] matchBlock(HashBlock block, int maxInterestingNumMatches) {
    if (block.getNumBasepairsUsed() < this.minInterestingSize)
      return null;
    PackedMap blocksOfThisSize = this.getContainingMap(block);
    if (blocksOfThisSize == null) {
      return emptyList;
    }
    int key2 = block.getLookupKey();
    boolean invert = !block.isPrimaryPolarity();
    SequencePosition[] results = blocksOfThisSize.get(key2, maxInterestingNumMatches);
    if (invert && results != null) {
      for (int i = 0; i < results.length; i++) {
        results[i] = reverseComplement(results[i], block.getLength());
      }
    }
    return results;
  }

  // This function does a lookup based on the hashcode and returns the blocks having this value as their forward hashcode
  public SequencePosition[] lookupByForwardHash(int blockLength, int hashKey) {
    PackedMap blocksOfThisSize = getContainingMap(blockLength);
    SequencePosition[] forwardMatches = blocksOfThisSize.get(hashKey, Integer.MAX_VALUE);
    if (forwardMatches == null)
      return null;
    SequencePosition[] allMatches = new SequencePosition[forwardMatches.length * 2];
    for (int i = 0; i < forwardMatches.length; i++) {
      allMatches[i] = forwardMatches[i];
      allMatches[i + forwardMatches.length] = reverseComplement(forwardMatches[i], blockLength);
    }
    return allMatches;
  }

  // given the start position of a hashblock match, returns the start position of the reverse complement
  private SequencePosition reverseComplement(SequencePosition position, int blockLength) {
    Sequence reverseComplement = this.database.getSequenceDatabase().getReverseComplement(position.getSequence());
    int startIndex = reverseComplement.getLength() - position.getStartIndex() - blockLength;
    return new SequencePosition(reverseComplement, startIndex);
  }

  public void ensureHashed(int blockLength) {
    getContainingMap(blockLength);
  }

  public int getNumHashKeys(int blockLength) {
    PackedMap blocksOfThisSize = getContainingMap(blockLength);
    if (blocksOfThisSize == null)
      return 0;
    return blocksOfThisSize.getCapacity();
  }

  public int getNumMatchesLowerBound(HashBlock block) {
    if (block.getNumBasepairsUsed() < this.minInterestingSize)
      return Integer.MAX_VALUE;
    PackedMap m = this.getContainingMap(block);
    if (m == null)
      return Integer.MAX_VALUE;
    int key2 = block.getLookupKey();
    return m.getNumMatchesLowerBound(block);
  }

  public int getMaxNumMatchesAllowed(HashBlock block) {
    if (block.getNumBasepairsUsed() < this.minInterestingSize) {
      return -1;
    }
    PackedMap m = this.getContainingMap(block);
    if (m == null)
      return 0;
    return m.getMaxInterestingCountPerKey();
  }

  public boolean knowsAllMatches(HashBlock block) {
    if (block.getNumBasepairsUsed() < this.minInterestingSize)
      return false;
    PackedMap m = this.getContainingMap(block);
    if (m == null) {
      return true;
    }
    int key2 = block.getLookupKey();
    return m.knowsAllMatches(key2);
  }

  // Creates and returns the PackedMap that should contain this HashBlock
  private PackedMap getContainingMap(HashBlock block) {
    return getContainingMap(block.getNumBasepairsUsed());
  }

  private PackedMap getContainingMap(int length) {
    if (this.maxHashedLength < length) {
      this.updateThroughSize(length);
    }
    return this.hashedBlocks.get(length);
  }

  private void updateThroughSize(int size) {
    this.database.updateThroughSize(this, size);
  }

  public void update(List<PackedMap> items, int maxHashedLength) {
    this.hashedBlocks = new ArrayList<PackedMap>(items);
    this.maxHashedLength = maxHashedLength;
  }


  // Gets this hashblock database ready for use
  // It's not required to call this method, but calling this method makes timing information more accurate because otherwise the first call to matchBlock will trigger hashing the reference
  public void prepare() {
    // Ask our database to hash at least 1 level. It will choose to hash more than that for more efficiency
    this.updateThroughSize(1);
  }

  // Helps our database do hashing but doesn't also update this wrapper
  public void helpSetUp() {
    this.database.helpSetUp();
  }

  public boolean getCanUseHelp() {
    return this.database.getCanUseHelp();
  }

  public int getHashedLength() {
    return this.maxHashedLength;
  }

  public boolean getEnableGapmers() {
    return this.database.getEnableGapmers();
  }

  public int getMinInterestingSize() {
    return this.minInterestingSize;
  }

  ArrayList<PackedMap> hashedBlocks;
  int maxHashedLength = -1;
  int minInterestingSize;
  HashBlock_Database database;
}
