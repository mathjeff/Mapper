package mapper;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

// a Counting_HashBlockPath is a HashBlockPath that keeps track of some metrics about which offsets are most popular
public class Counting_HashBlockPath {
  static long[] emptyList = new long[0];

  public Counting_HashBlockPath(HashBlock_Pyramid pyramid, Readable_HashBlock_Database database, SequenceDatabase sequenceDatabase, Sequence query, String queryShortName, Logger logger, AlignmentParameters alignmentParameters) {
    this.path = new HashBlockPath(pyramid, database, sequenceDatabase, query, logger.incrementScope(), queryShortName);
    this.pyramid = pyramid;
    this.database = database;
    this.sequenceDatabase = sequenceDatabase;
    this.query = query;
    this.reverseComplementQuery = query.reverseComplement();
    this.queryShortName = queryShortName;
    this.logger = logger;
    this.targetBlockLength = (int)(Math.log((double)this.sequenceDatabase.getTotalForwardAndReverseSize()) / Math.log(4.0)) + 1;

    // If there's an indel in the best alignment, we might get some hashblock matches at slightly different offsets that each corresponding to that alignment
    // We consider nearby offsets together when counting evidence for a particular offset
    int maxPossibleIndel = (int)((query.getLength() * alignmentParameters.MaxErrorRate - alignmentParameters.DeletionStart_Penalty) / alignmentParameters.DeletionExtension_Penalty);
    // If all of the penalty for an alignment is used for a single indel, then there won't be any other mutations, and the alignment should be not too difficult to find. So, it shouldn't be required for us to combine offsets that differ by the full indel length
    // Instead, we only combine closer offsets because this should be less likely to report false positives for highly duplicated queries
    this.maxIndelLengthToConsider = maxPossibleIndel / 2;
  }

  // advance the path, update the match counts, and return whether we made any progress
  public boolean step() {
    if (this.done)
      return false;
    if (this.logger.getEnabled()) {
      this.logger.log("");
      this.logger.log("Finding next " + this.queryShortName + " match:");
    }
    HashBlock_Match match = this.getNextInterestingMatch();
    if (match == null) {
      if (this.logger.getEnabled()) {
        this.logger.log(this.queryShortName + " Counting_HashBlockPath done");
      }
      this.done = true;
      if (this.numBlocksMatchingAnywhere < 2) {
        // If we only found one hashblock that matched anywhere, then the total number of matches of all of our hashblocks should be small
        // Also, the query itself should be small so it should be easy to check an individual alignment
        // So, if we only found one hashblock that matched anywhere, we allow checking for alignments at the places where it matched
        // This helps us to detect good alignments for short queries
        this.tryEnsureGoodMatchCounter();
      }
      return false; // no more work left to do
    }
    HashBlock queryBlock = match.block;
    if (this.logger.getEnabled()) {
      this.logger.log(this.queryShortName + "[" + queryBlock.getStartIndex() + ":" + queryBlock.getEndIndex() + "]  =  " + spaces(queryBlock.getStartIndex()) + queryBlock.getText(this.query));
      if (queryBlock != null) {
        this.logger.log("Hash code = " + queryBlock.getForwardHash() + ", reports matches in " + match.matches.length + " locations:");
      }
    }
    this.interestingMatch_history.add(queryBlock);
    for (SequencePosition referenceBlock: match.matches) {
      if (this.logger.getEnabled()) {
        int startIndex = referenceBlock.getStartIndex();
        String referenceText;
        if (startIndex < 0 || startIndex + queryBlock.getLength() > referenceBlock.getSequence().getLength()) {
          referenceText = "invalid";
        } else {
          referenceText = queryBlock.getTextAt(referenceBlock.getSequence(), referenceBlock.getStartIndex());
        }
        int offset = (referenceBlock.getStartIndex() - queryBlock.getStartIndex());
        String messageText = " " + referenceBlock.getSequence().getName() + " offset " + offset;
        if (referenceBlock.getSequence().getComplementedFrom() != null) {
          Sequence forwardReference = referenceBlock.getSequence().getComplementedFrom();
          int forwardReferenceStartIndex = forwardReference.getLength() - (offset + query.getLength());
          messageText += " (reverse offset " + forwardReferenceStartIndex + ")";
        }
        messageText += " -> " + referenceText;
        this.logger.log(messageText);
      }

      // Create a Match object pointing to the sequence and position
      // TODO: can we do fewer allocations?
      Sequence currentMatchedSequence = referenceBlock.getSequence();

      int numMismatchedItems = 0;
      int numMatchedItems = 0;
      // do a brief check to try to skip non-matching positions (hash collisions)
      for (int middleIndexOffset = -3; middleIndexOffset <= queryBlock.getLength() + 3; middleIndexOffset++) {
        if (middleIndexOffset >= 0 && middleIndexOffset <= queryBlock.getLength())
          middleIndexOffset += queryBlock.getLength() + 1;
        int queryIndex = queryBlock.getStartIndex() + middleIndexOffset;
        if (queryIndex < 0 || queryIndex >= query.getLength())
          continue;
        int referenceIndex = referenceBlock.getStartIndex() + middleIndexOffset;
        if (referenceIndex < 0 || referenceIndex >= currentMatchedSequence.getLength())
          continue;
        byte encodedQueryChar = this.query.encodedCharAt(queryIndex);
        byte encodedRefChar = currentMatchedSequence.encodedCharAt(referenceIndex);
        if (!Basepairs.canMatch(encodedQueryChar, encodedRefChar)) {
          if (logger.getEnabled()) {
            logger.log("   checked basepair " + middleIndexOffset + " in query block: got " + Basepairs.decode(encodedQueryChar) + " in query and " + Basepairs.decode(encodedRefChar) + " in reference");
          }
          // Query block detected as different from reference block (hash collision). Skipping
          numMismatchedItems++;
        } else {
          numMatchedItems++;
        }
      }
      if (numMismatchedItems > numMatchedItems) {
        if (logger.getEnabled())
          logger.log("  skipping probable hash collision");
        continue;
      }
      SequenceMatch fullMatch;
      if (currentMatchedSequence.getComplementedFrom() != null) {
        Sequence forwardRef = currentMatchedSequence.getComplementedFrom();
        int reverseQueryBlockStart = query.getLength() - queryBlock.getEndIndex();
        int reverseReferenceBlockStart = currentMatchedSequence.getLength() - (referenceBlock.getStartIndex() + queryBlock.getLength());
        int reverseLocalOffset = reverseReferenceBlockStart - reverseQueryBlockStart;

        fullMatch = new SequenceMatch(reverseComplementQuery, forwardRef, reverseLocalOffset);
      } else {
        int currentLocalOffset = referenceBlock.getStartIndex() - queryBlock.getStartIndex();

        fullMatch = new SequenceMatch(query, currentMatchedSequence, currentLocalOffset);
      }
      this.updateMatches(fullMatch, queryBlock);
    }
    if (queryBlock.getStartIndex() >= this.maxNonoverlappingBlockVisited) {
      this.maxNonoverlappingBlockVisited = queryBlock.getEndIndex();
      this.numNonoverlappingBlocksVisited++;
    }
    this.numBlocksMatchingAnywhere++;
    int numMatchesHere = match.getMatches().length;
    if (numMatchesHere > 0)
      this.latestNumGoodHashblockMatches = numMatchesHere;
    this.minNumDistinctMismatches = -1; // invalidate so we recalculate later if needed
    return true; // there may be more work left to do
  }

  private HashBlockMatch_Counter getCounter(Map<Integer, HashBlockMatch_Counter> map, int position, SequenceMatch match, HashBlock queryBlock) {
    HashBlockMatch_Counter counter = map.get(position);
    return counter;
  }

  private HashBlockMatch_Counter createCounter(Map<Integer, HashBlockMatch_Counter> map, int position, SequenceMatch match, HashBlock queryBlock) {
    HashBlockMatch_Counter counter = new HashBlockMatch_Counter(match, this.interestingMatch_history, this.numNonoverlappingBlocksVisited, queryBlock.getStartIndex());
    map.put(position, counter);
    return counter;
  }

  private void updateMatches(SequenceMatch sequenceMatch, HashBlock queryBlock) {
    Sequence sequence = sequenceMatch.getSequenceB();
    int offset = sequenceMatch.getOffset();
    Map<Sequence, TreeMap<Integer, HashBlockMatch_Counter>> allMatchCounters;
    if (sequenceMatch.getReversed())
      allMatchCounters = this.forwardMatchCounters;
    else
      allMatchCounters = this.reverseMatchCounters;

    TreeMap<Integer, HashBlockMatch_Counter> matchesOnSequence = allMatchCounters.get(sequence);
    if (matchesOnSequence == null) {
      matchesOnSequence = new TreeMap<Integer, HashBlockMatch_Counter>();
      allMatchCounters.put(sequence, matchesOnSequence);
    }

    HashBlockMatch_Counter currentCounter = getCounter(matchesOnSequence, offset, sequenceMatch, queryBlock);
    if (currentCounter == null) {
      // create counter
      currentCounter = createCounter(matchesOnSequence, offset, sequenceMatch, queryBlock);

      // update previous neighbor
      Map.Entry<Integer, HashBlockMatch_Counter> previousCounterEntry = matchesOnSequence.lowerEntry(offset);
      if (previousCounterEntry != null) {
        int previousOffset = previousCounterEntry.getKey();
        HashBlockMatch_Counter previousCounter = previousCounterEntry.getValue();
        if (Math.abs(previousOffset - sequenceMatch.getOffset()) <= this.maxIndelLengthToConsider) {
          currentCounter.setPreviousCounter(previousCounter);
          previousCounter.setNextCounter(currentCounter);
        }
      }

      // update next neighbor
      Map.Entry<Integer, HashBlockMatch_Counter> nextCounterEntry = matchesOnSequence.higherEntry(offset);
      if (nextCounterEntry != null) {
        int nextOffset = nextCounterEntry.getKey();
        HashBlockMatch_Counter nextCounter = nextCounterEntry.getValue();
        if (Math.abs(nextOffset - sequenceMatch.getOffset()) <= this.maxIndelLengthToConsider) {
          currentCounter.setNextCounter(nextCounter);
          nextCounter.setPreviousCounter(currentCounter);
        }
      }
    }

    HashBlockMatch_Counter previousCounter = currentCounter.getPreviousCounter();
    if (previousCounter != null)
      this.addMatch(sequenceMatch, queryBlock, previousCounter);
    HashBlockMatch_Counter nextCounter = currentCounter.getNextCounter();
    if (nextCounter != null)
      this.addMatch(sequenceMatch, queryBlock, nextCounter);

    // If we're already keeping track of nearby matches then we don't have to keep track of this one
    boolean updateThisOne = true;
    if ((previousCounter != null && previousCounter.isGood()) || (nextCounter != null && nextCounter.isGood())) {
      if (!currentCounter.isGood()) {
        updateThisOne = false;
      }
    }
    if (updateThisOne)
      this.addMatch(sequenceMatch, queryBlock, currentCounter);
  }

  private void addMatch(SequenceMatch fullMatch, HashBlock queryBlock, HashBlockMatch_Counter counter) {
    int currentCount;
    counter.addMatch(fullMatch, queryBlock);
    counter.update();
    // once a counter has enough matches, we can consider it good
    if (counter.getNumMatches() <= 2) {
      if (counter.getNumMatches() == 2) {
        this.foundGoodMatchCounter = true;
        this.declareGood(counter);
      } else {
        int distanceFromStart, distanceFromEnd;
        distanceFromStart = fullMatch.getOffset();
        distanceFromEnd = fullMatch.getSequenceB().getLength() - (fullMatch.getOffset() + fullMatch.getSequenceA().getLength());
        int distanceFromEdge = Math.min(distanceFromStart, distanceFromEnd);
        if (distanceFromEdge < 0) {
          this.declareGood(counter);
        }
      }
    }
  }

  // declares that this hashblock match is worth considering as a potential alignment offset
  private void declareGood(HashBlockMatch_Counter counter) {
    if (!counter.isGood()) {
      this.goodMatchCounters.add(counter);
      counter.setGood();
    }
  }

  // If we don't already have a hashblock match that we consider good, this function declares all of them to be good

  // This function is more helpful for shorter queries with more mutations, and is more likely to trigger for those queries
  // This function is more expensive for longer queries, but less likely to trigger for those queries
  public void tryEnsureGoodMatchCounter() {
    if (!this.foundGoodMatchCounter) {
      if (this.logger.getEnabled()) {
        this.logger.log("declaring all matches to be good in an effort to try some lookups");
      }
      for (Map<Integer, HashBlockMatch_Counter> countersOnSequence: forwardMatchCounters.values()) {
        for (HashBlockMatch_Counter counter: countersOnSequence.values()) {
          this.declareGood(counter);
        }
      }
      for (Map<Integer, HashBlockMatch_Counter> countersOnSequence: reverseMatchCounters.values()) {
        for (HashBlockMatch_Counter counter: countersOnSequence.values()) {
          this.declareGood(counter);
        }
      }
      this.foundGoodMatchCounter = true;
    }
  }

  // For diagnostic information only
  // Computes the possible hashes that might correspond to a specific sequence
  // We could save this information and reload it, but most of the time we don't need it, and it requires a bunch of memory to save it, so instead we recompute it when the user wants extra-verbose output
  private String formatPossibleHashes(Sequence sequence, int queryHash) {
    String result = "hash =";
    boolean matches = false;
    HashBlock_Stream stream = new HashBlock_Stream(sequence, true, null);
    while (true) {
      HashBlock_Row row = stream.getNextBatch();
      if (row == null) {
        break;
      }
      IMultiHashBlock multiHashBlock = row.get(0);
      if (multiHashBlock == null) {
        break;
      }
      for (ConditionalHashBlock possibility : multiHashBlock.getPossibilities()) {
        HashBlock hashBlock = possibility.getHashBlock();
        if (hashBlock != null) {
          if (hashBlock.getStartIndex() == 0 && hashBlock.getEndIndex() == sequence.getLength()) {
            if (hashBlock.getForwardHash() == queryHash)
              matches = true;
            result = result + " " + hashBlock.getForwardHash();
          }
        }
      }
    }
    if (matches)
      result += " (same hash)";
    else
      result += " (different hash)";
    return result;
  }

  private HashBlock getNextInterestingBlock() {
    this.previousAllPositions = null;
    while (true) {

      HashBlock block = this.path.getNextInterestingBlock();

      if (block == null) {
        if (this.pendingBlocks.size() < 1)
          return null;
        return this.pendingBlocks.remove();
      }

      boolean overlap = block.getStartIndex() < this.maxNonoverlappingBlockVisited;
      if (overlap) {
        if (this.logger.getEnabled()) {
          this.logger.log(this.queryShortName + "[" + block.getStartIndex() + ":" + block.getEndIndex() + "]  =  " + spaces(block.getStartIndex()) + block.getText(this.query) + " overlaps earlier block; saving for later");
        }

        this.pendingBlocks.add(block);
        continue;
      }

      return block;
    }
  }

  // steps to the next interesting HashBlock_Match in the path
  private HashBlock_Match getNextInterestingMatch() {
    while (true) {
      HashBlock block = this.getNextInterestingBlock();
      if (block == null) {
        return null;
      } else {
        SequencePosition[] matches = this.queryDatabase(block);
        if (matches == null)
          continue;
        HashBlock_Match match = new HashBlock_Match(block, matches);
        return match;
      }
    }
  }

  private SequencePosition[] queryDatabase(HashBlock block) {
    return database.matchBlock(block);
  }

  // Returns the topmost hashblock in our HashBlockPyramid
  // If the top level has multiple hashblocks, chooses arbitrarily but deterministically
  private IMultiHashBlock getTopHashblock() {
    IMultiHashBlock latest = null;
    int level = 0;
    while (true) {
      HashBlock_Row batch = this.pyramid.get(level);
      IMultiHashBlock firstInRow = batch.getAfter(-1);
      if (firstInRow == null) {
        return latest;
      }
      latest = firstInRow;
      level++;
    }
  }

  public List<HashBlockMatch_Counter> findGoodPositionsHavingPriorityUpTo(int priority) {
    // advance far enough that we can know how many positions there are having this number of mismatches
    while (true) {
      if (numNonoverlappingBlocksVisited >= priority + 2) {
        // If a position has 2 distinct hashblocks then we count it as a good position
        // So, if we want to find up to <numMismatches> mismatched blocks, we have to check up to 2 more blocks
        break;
      }
      if (!this.step()) {
        break;
      }
    }

    // We process priorities in order (from best (0) to worst)
    // So, if we previously (better priority) returned all of the good match counters, the result now will be the same too
    if (previousHighPriorityMatchCounters != null && previousHighPriorityMatchCounters.size() == goodMatchCounters.size()) {
      return this.previousHighPriorityMatchCounters;
    }

    List<HashBlockMatch_Counter> matches = new ArrayList<HashBlockMatch_Counter>();
    for (HashBlockMatch_Counter counter : this.goodMatchCounters) {
      if (counter.getPriority() <= priority) {
        matches.add(counter);
      }
    }
    this.previousHighPriorityMatchCounters = matches;
    return matches;
  }

  public List<HashBlockMatch_Counter> getAllPositions() {
    if (this.previousAllPositions == null) {
      ArrayList<HashBlockMatch_Counter> results = new ArrayList<HashBlockMatch_Counter>();
      for (Map<Integer, HashBlockMatch_Counter> countersOnSequence: forwardMatchCounters.values()) {
        for (HashBlockMatch_Counter counter: countersOnSequence.values()) {
          results.add(counter);
        }
      }
      for (Map<Integer, HashBlockMatch_Counter> countersOnSequence: reverseMatchCounters.values()) {
        for (HashBlockMatch_Counter counter: countersOnSequence.values()) {
          results.add(counter);
        }
      }
      this.previousAllPositions = results;
    }
    return this.previousAllPositions;
  }

  public int getNumBlocks() {
    return numBlocksMatchingAnywhere;
  }

  private int getNumGoodDistinctMismatches() {
    if (this.minNumDistinctMismatches < 0) {
      int min = this.numNonoverlappingBlocksVisited - 1;
      for (HashBlockMatch_Counter counter : this.goodMatchCounters) {
        int count = counter.getNumDistinctMismatches();
        if (min >= count) {
          min = count;
        }
      }
      this.minNumDistinctMismatches = min;
    }
    return this.minNumDistinctMismatches;
  }

  public List<HashBlockMatch_Counter> getBestMatches() {
    List<HashBlockMatch_Counter> best = new ArrayList<HashBlockMatch_Counter>();
    if (numBlocksMatchingAnywhere < 2) {
      return best;
    }
    int min = this.getNumGoodDistinctMismatches();
    for (HashBlockMatch_Counter counter : this.goodMatchCounters) {
      int count = counter.getNumDistinctMismatches();
      if (count <= min)
        best.add(counter);
    }
    if (this.logger.getEnabled()) {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("" + best.size() + " best " + this.queryShortName + " positions found (having num mismatches = " + min + " ");
      for (HashBlockMatch_Counter counter: best) {
        stringBuilder.append(counter.getMatch().getSequenceB().getName());
        stringBuilder.append(" offset " + counter.getMatch().getOffset() + " ");
      }
      stringBuilder.append(")");
      logger.log(stringBuilder.toString());
    }
    return best;
  }

  public String getQueryShortName() {
    return this.queryShortName;
  }

  public Sequence getQuerySequence() {
    return this.query;
  }

  public boolean isDone() {
    return this.done;
  }

  private String spaces(int count) {
    String result = "";
    for (int i = 0; i < count; i++) {
      result += " ";
    }
    return result;
  }

  HashBlockPath path;
  HashBlock_Pyramid pyramid;
  Readable_HashBlock_Database database;
  // Map<rounded offset, count>
  Map<Sequence, TreeMap<Integer, HashBlockMatch_Counter>> forwardMatchCounters = new HashMap<Sequence, TreeMap<Integer, HashBlockMatch_Counter>>();
  Map<Sequence, TreeMap<Integer, HashBlockMatch_Counter>> reverseMatchCounters = new HashMap<Sequence, TreeMap<Integer, HashBlockMatch_Counter>>();
  List<HashBlockMatch_Counter> goodMatchCounters = new ArrayList<HashBlockMatch_Counter>();
  boolean foundGoodMatchCounter;
  List<HashBlock> interestingMatch_history = new ArrayList<HashBlock>();
  int numBlocksMatchingAnywhere;
  int numBlocksMatchingInTheSamePlace;
  SequenceDatabase sequenceDatabase;
  Sequence query;
  Sequence reverseComplementQuery;
  Logger logger;

  // the end index of the latest nonoverlapping block that we've seen
  int maxNonoverlappingBlockVisited;
  // the number of nonoverlapping blocks that we've seen
  int numNonoverlappingBlocksVisited;
  //
  int latestNumGoodHashblockMatches;

  int minNumDistinctMismatches = -1;

  boolean done = false;
  int targetBlockLength;

  int maxIndelLengthToConsider;
  String queryShortName;

  Queue<HashBlock> pendingBlocks = new ArrayDeque<HashBlock>();

  List<HashBlockMatch_Counter> previousHighPriorityMatchCounters;
  List<HashBlockMatch_Counter> previousAllPositions;
}
