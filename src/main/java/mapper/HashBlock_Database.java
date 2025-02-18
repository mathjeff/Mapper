package mapper;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

// A HashBlock_Database is a collection of HashBlock objects.
// If two HashBlocks have the same text, they are considered equal.
public class HashBlock_Database implements ReferenceProvider {
  public HashBlock_Database(SequenceDatabase sequences) {
    this.initialize(sequences, -1, -1, -1, true, null, new StatusLogger(new Logger(new mapper.StderrWriter()), 0));
  }

  public HashBlock_Database(SequenceDatabase sequences, StatusLogger statusLogger) {
    this.initialize(sequences, -1, -1, -1, true, null, statusLogger);
  }

  public HashBlock_Database(SequenceDatabase sequences, int minInterestingSize, int hintMaxInterestingSize, int maxNumShortMatches, boolean enableGapmers, DirCache dirCache, StatusLogger statusLogger) {
    this.initialize(sequences, minInterestingSize, hintMaxInterestingSize, maxNumShortMatches, enableGapmers, dirCache, statusLogger);
  }

  private void initialize(SequenceDatabase sequences, int minInterestingSize, int hintMaxInterestingSize, int maxNumShortMatches, boolean enableGapmers, DirCache dirCache, StatusLogger statusLogger) {
    this.logger = statusLogger.getLogger();
    this.statusLogger = statusLogger;
    this.enableGapmers = enableGapmers;
    for (Sequence sequence : sequences.getAll()) {
      this.addSequence(sequence);
    }
    this.sequenceDatabase = sequences;
    if (this.logger.getEnabled())
      this.logger.log("total reference size: " + this.totalForwardSize * 2);
    if (minInterestingSize < 0) {
      this.minInterestingSize = (int)Math.max((Math.log(this.totalForwardSize + 1) / Math.log(4)) - 2, 0);
    } else {
      this.minInterestingSize = minInterestingSize;
    }
    if (hintMaxInterestingSize > 0) {
      if (sequences.getTotalForwardSize() > 1000000000L) {
        // The reference sequences are really large and we might not have enough memory
        // Start by hashing a smaller size to reduce the maximum memory usage (due to uncompressed pending adds)
        // After hashing the smaller size we can hash the larger size
        this.maxInterestingSize = (hintMaxInterestingSize + 1) / 2;
      } else {
        // The reference sequences don't seem to be too large so we try to save all of the interesting hashblocks in the first pass over the reference
        this.maxInterestingSize = hintMaxInterestingSize;
      }
    } else {
      this.maxInterestingSize = -1;
    }
    if (maxNumShortMatches < 0) {
      // Advancing to the next level approximately multiplies the number of hashblocks by about 3/4.

      // We want to keep these factors down:
      // 1. The time creating hashblocks
      //    When we advance to the next level this requires roughly 1 time unit
      // 2. The time visiting hashblock matches
      //    When we advance to the next level this requires roughly 3/4 as much time
      // 3. The time demonstrating that we've found the optimal alignment
      //    With larger hashblocks we're less likely to be able to prove the full penalty as easily

      // Minimizing #1 + #2 happens at roughly 4 matches per hashblock
      // Factor #3 favors smaller hashblocks, which tend to have more matches
      // We also want to have a high probability to detect the optimal alignment, which also favors more matches

      this.maxNumShortMatches = 5;
    } else {
      this.maxNumShortMatches = maxNumShortMatches;
    }
    this.cacheDir = this.chooseCacheDir(dirCache);
    this.chooseNextHashSize(0);
  }

  // choose a directory to save this database into
  private File chooseCacheDir(DirCache dirCache) {
    if (dirCache == null)
      return null;
    try {
      TreeMap<String, String> keys = this.getCacheKeys();
      return dirCache.getOrCreateDir(keys);
    } catch (IOException e) {
      // If we're supposed to choose a cache directory and we can't, that's a fatal error
      throw new RuntimeException(e);
    }
  }

  public TreeMap<String, String> getCacheKeys() {
    TreeMap<String, String> keys = new TreeMap<String, String>(this.sequenceDatabase.getCacheKeys());
    keys.put("enableGapmers", "" + this.enableGapmers);
    keys.put("minInterestingSize", "" + this.minInterestingSize);
    keys.put("maxNumShortMatches", "" + this.maxNumShortMatches);
    keys.put("formatVersion", "1");
    keys.put("type", "HashBlock_Database");
    return keys;
  }

  public HashBlock_Database get_HashBlock_database(Logger logger) {
    return this;
  }

  public SequenceDatabase getSequenceDatabase() {
    return this.sequenceDatabase;
  }

  public Sequence getOriginalSequence(Sequence sequence) {
    // HashBlock_Database doesn't modify sequences
    return sequence;
  }

  // Returns a new view of this database to allow usage from a separate thread
  // If multiple threads need access to this database, each one should interact solely with its own view
  public Readable_HashBlock_Database getView() {
    return new Readable_HashBlock_Database(this);
  }

  private void addSequence(Sequence sequence) {
    if (sequence.getComplementedFrom() == null)
      this.totalForwardSize += sequence.getLength();
  }

  // Tells whether having another thread help hash this database could allow it to respond to an existing query more quickly
  public boolean getCanUseHelp() {
    synchronized(this) {
      return needsMoreSetup();
    }
  }

  // called by a thread that needs to have hashed all streams through this size
  private void requireSetUpThroughSize(int size) {
    while (true) {
      synchronized(this) {
        if (this.maxFullySetUpSize >= size) {
          return;
        }
        if (maxFullySetUpSize >= this.maxInterestingSize) {
          // Completed previous hashing so now we can hash again
          this.chooseNextHashSize(size);
        }
      }
      helpSetUp();

      // sleep if we're simply waiting for other threads to hash their sequences
      boolean sleep;
      synchronized (this) {
        sleep = this.needsMoreSetup();
      }
      if (sleep) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
        }
      }
    }
  }

  private boolean needsMoreSetup() {
    if (maxFullySetUpSize < 1)
      return true;
    if (maxFullySetUpSize < this.maxInterestingSize)
      return true;
    return false;
  }

  private void chooseNextHashSize(int requestSize) {
    if (this.maxFullySetUpSize < 1) {
      // we haven't hashed anything yet
      if (this.maxInterestingSize < 0) {
        // don't have a default max interesting size yet
        int initialSize = DuplicationDetector.chooseMaxDuplicationLength(this.sequenceDatabase);
        this.maxInterestingSize = Math.max(initialSize, requestSize);
      }
    } else {
      this.maxInterestingSize = requestSize * 2;
    }
    if (this.logger.getEnabled())
      this.logger.log("hashing lengths " + (this.maxFullySetUpSize + 1) + " - " + this.maxInterestingSize);
    this.lengthsLeftToLoad = new ArrayDeque<Integer>();
    if (this.cacheDir != null) {
      this.minNonloadableLength = this.maxInterestingSize + 1;

      for (int i = Math.max(this.minInterestingSize, this.maxFullySetUpSize + 1); i <= this.maxInterestingSize; i++) {
        File cacheFile = this.chooseMapCacheFile(i);
        if (cacheFile.exists()) {
          // If the cache file exists we can try to load it
          this.lengthsLeftToLoad.add(i);
        } else {
          this.minNonloadableLength = i;
          // If the cache file doesn't exist, there's no point in trying to load it and outputting an error
          break;
        }
      }
    }
    this.cumulativeHashedSize = 0;
    // Also list the sequences that we will need to hash if loading from the cache isn't sufficient
    this.sequencesLeftToHash = new ArrayDeque<Sequence>(this.sequenceDatabase.getForwardSequencesOnly());
  }

  // called by a thread to contribute to setting up
  public void helpSetUp() {
    this.helpLoad();
    this.helpHash();
    this.helpPack();
  }

  // helps load the reference from cache and waits until done loading
  private void helpLoad() {
    if (this.cacheDir == null) {
      return;
    }

    while (true) {
      boolean sleep = false;
      synchronized(this) {
        if (this.lengthsLeftToLoad.size() < 1) {
          if (this.numActiveLoaders < 1) {
            return; // done loading
          } else {
            // still waiting for some other workers
            sleep = true;
          }
        }
      }
      if (sleep) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
        }
      }
      helpLoadOnce();
    }
  }

  private void helpLoadOnce() {
    int numBasepairsUsed;
    synchronized(this) {
      if (this.lengthsLeftToLoad.size() < 1) {
        return;
      }
      numBasepairsUsed = this.lengthsLeftToLoad.remove();
      this.numActiveLoaders++;
    }

    File cacheFile = chooseMapCacheFile(numBasepairsUsed);
    PackedMap map = null;
    try {
      map = new PackedMap(cacheFile, this.sequenceDatabase);
    } catch (Exception e) {
      if (this.logger.getEnabled()) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        e.printStackTrace(printWriter);
        this.logger.log("Could not load map from " + cacheFile + ": " + stringWriter.toString());
      }
      this.minNonloadableLength = Math.min(this.minNonloadableLength, numBasepairsUsed);
    }
    synchronized(this) {
      this.numActiveLoaders--;
      // save this map
      while (this.hashedBlocks.size() <= numBasepairsUsed)
        this.hashedBlocks.add(null);
      this.hashedBlocks.set(numBasepairsUsed, map);
      if (this.numActiveLoaders < 1 && this.lengthsLeftToLoad.size() < 1) {
        // Done trying to load
        // Clear any maps starting from the first failure, if any
        for (int i = this.minNonloadableLength; i < this.hashedBlocks.size(); i++) {
          this.hashedBlocks.set(i, null);
        }

        if (this.minNonloadableLength > this.minInterestingSize) {
          this.statusLogger.log("HashBlock_Database loaded lengths through " + (this.minNonloadableLength - 1) + " from " + this.cacheDir, true);
        } else {
          this.statusLogger.log("HashBlock_Database loaded no cache entries from " + this.cacheDir, true);
        }
        if (this.minNonloadableLength > this.maxInterestingSize) {
          // We loaded each PackedMap and don't need to re-hash any sequences
          this.sequencesLeftToHash = new ArrayDeque<Sequence>();
        }
        // Record that we don't need to rehash any lengths that we loaded successfully
        this.maxFullySetUpSize = Math.max(this.maxFullySetUpSize, this.minNonloadableLength - 1);
      }
    }
  }

  // helps hash the reference and waits until done hashing
  private void helpHash() {
    while (true) {
      boolean sleep = false;
      synchronized(this) {
        if (this.sequencesLeftToHash.size() < 1) {
          if (this.numActiveHashers < 1) {
            return; // done
          } else {
            // still waiting for some other workers
            sleep = true;
          }
        }
      }
      if (sleep) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
        }
      }
      helpHashOnce();
    }
  }

  // does one iteration of helping hash the reference sequences
  private void helpHashOnce() {
    Sequence sequence;
    int size;
    synchronized(this) {
      size = this.maxInterestingSize;
      if (this.sequencesLeftToHash.size() < 1) {
        return;
      }
      sequence = this.sequencesLeftToHash.remove();
      this.numActiveHashers++;
    }
    this.hashSequenceThroughSize(sequence, size);
    synchronized(this) {
      int previousNumActiveHashers = this.numActiveHashers;
      this.numActiveHashers--;
      int numSequencesRemaining = this.sequencesLeftToHash.size() + this.numActiveHashers;
      this.cumulativeHashedSize += sequence.getLength();
      long percentComplete = this.cumulativeHashedSize * 100 / this.totalForwardSize;
      int numForwardSequences = this.sequenceDatabase.getNumSequences() / 2;
      int completionIndex = numForwardSequences - numSequencesRemaining;
      boolean important = (numSequencesRemaining == 0);
      this.statusLogger.log("Hashed contig " + completionIndex + "/" + numForwardSequences + " (" + percentComplete + "%) with " + previousNumActiveHashers + " active workers", important);
      if (numSequencesRemaining < 1) {
        while (size >= this.hashedBlocks.size())
          this.hashedBlocks.add(null);
        for (int i = 0; i <= size; i++) {
          PackedMap row = this.hashedBlocks.get(i);
          if (row == null) {
            row = new PackedMap(1, 1, this.sequenceDatabase, i);
            this.hashedBlocks.set(i, row);
          }
        }
        this.statusLogger.log("Hashed reference through size " + this.maxInterestingSize, true);
        for (int i = this.maxFullySetUpSize + 1; i <= size; i++) {
          PackedMap map = this.hashedBlocks.get(i);
          if (map != null)
            this.mapsLeftToPack.add(map);
        }
        this.statusLogger.log("Compressing blocks with length through " + this.maxInterestingSize, true);
      }
    }
  }

  private void helpPack() {
    while (true) {
      synchronized(this) {
        if (this.mapsLeftToPack.size() < 1) {
          return;
        }
      }
      helpPackOnce();
    }
  }

  private void helpPackOnce() {
    PackedMap map = null;
    synchronized(this) {
      if (this.mapsLeftToPack.size() < 1)
        return;
      map = mapsLeftToPack.remove();
      this.numActivePackers++;
    }
    map.pack();
    try {
      this.exportMap(map);
    } catch (IOException e) {
      // If we're supposed to save the map but we can't, then we just report a failure
      throw new RuntimeException(e);
    }
    // Maybe dump to cache here too
    synchronized(this) {
      this.numActivePackers--;
      if (mapsLeftToPack.size() < 1 && this.numActivePackers < 1) {

        int cumulativeCapacity = 0;
        while (this.maxInterestingSize >= this.hashedBlocks.size())
          this.hashedBlocks.add(null);
        for (int i = 0; i <= this.maxInterestingSize; i++) {
          PackedMap row = this.hashedBlocks.get(i);
          cumulativeCapacity += row.getCapacity();
          if (row.getCapacity() > 1) {
            if (this.logger.getEnabled())
              this.logger.log("Hashed length " + i + " into " + row.getCapacity() + " bins, saturated " + row.getNumOverfilledKeys() + " bins (cumulative capacity " + cumulativeCapacity + ") (num items added here " + row.getNumItemsAdded() + ") in " + row.getTotalAddMillis() + "ms");
          }
        }

        this.maxFullySetUpSize = this.maxInterestingSize;
        this.statusLogger.log("Processed reference through size " + this.maxFullySetUpSize, true);
        if (this.cacheDir != null) {
          if (this.logger.getEnabled()) {
            this.logger.log("HashBlock_Database saved to " + this.cacheDir);
          }
        }
      }
    }
  }

  private void exportMap(PackedMap map) throws IOException {
    if (this.cacheDir == null)
      return;
    int numBasepairsUsed = map.getId();
    File destFile = chooseMapCacheFile(numBasepairsUsed);
    map.writeTo(destFile);
  }

  private File chooseMapCacheFile(int numBasepairsUsed) {
    return new File(this.cacheDir, "length-" + numBasepairsUsed);
  }

  private void hashSequenceThroughSize(Sequence sequence, int size) {
    HashBlock_Buffer buffer = new HashBlock_Buffer(sequence, this, this.minInterestingSize);
    HashBlock_Stream stream = new HashBlock_Stream(sequence, true, buffer);

    HashBlock_Pyramid pyramid = new HashBlock_Pyramid(stream);

    int level = 0;
    int offset = -1;

    //HashBlock_Row batch = stream.getNextBatch();

    // Visit every block in this batch, which will trigger the visiting of all blocks in all other batches
    // Each batch (including this one) will call this.addBlock() for each discovered block
    //HashBlock block = batch.getAfter(-1);
    while (true) {
      HashBlock_Row batch = pyramid.get(level);
      IMultiHashBlock block = batch.getAfter(offset);
      if (block == null)
        break;
      if (block.getMinLength() <= size) {
        // also have to process the next batch because it might also have interesting blocks
        level++;
        offset = block.getStartIndex() - 1;
      } else {
        level--;
        offset = block.getStartIndex();

        int blockIndex = block.getStartIndex();
        batch.garbageCollect(blockIndex);
      }
    }
    buffer.flush();
  }

  public void addHashblocks(Sequence sequence, List<IMultiHashBlock> blocks) {
    // first group the hashblocks by size
    boolean containsAmbiguousPosition = false;
    Map<Integer, List<HashBlock>> blocksBySize = new HashMap<Integer, List<HashBlock>>();
    for (IMultiHashBlock multiblock : blocks) {
      HashBlock block = multiblock.getSingle();
      if (block != null) {
        // The common case: this MultiHashBlock is just a HashBlock
        addToBlocksBySize(block, sequence, blocksBySize);
      } else {
        // This MultiHashBlock contains multiple conditional hashblocks
        containsAmbiguousPosition = true;
        for (ConditionalHashBlock possibility : multiblock.getPossibilities()) {
          HashBlock possibleBlock = possibility.getHashBlock();
          if (possibleBlock != null) {
            addToBlocksBySize(possibleBlock, sequence, blocksBySize);
          }
        }
      }
    }

    // Now add each group of hashblocks to the corresponding PackedMap
    for (Map.Entry<Integer, List<HashBlock>> entry : blocksBySize.entrySet()) {
      int numBasepairsUsed = entry.getKey();
      int key1 = numBasepairsUsed;
      List<HashBlock> blocksHere = entry.getValue();

      PackedMap blocksOfThisSize = null;
      synchronized(this) {
        if (key1 < this.hashedBlocks.size())
          blocksOfThisSize = this.hashedBlocks.get(key1);
        if (blocksOfThisSize == null) {
          int estimatedCapacity = estimateRequiredCapacity(numBasepairsUsed);

          // Decide how many matches we allow in this map

          // required number of base pairs to identify a specific position in a random reference genome
          int targetBlockLength = (int)(Math.log((double)this.sequenceDatabase.getTotalForwardAndReverseSize()) / Math.log(4.0)) + 1;

          int maxNumInterestingMatches = numBasepairsUsed * numBasepairsUsed;
          if (maxNumInterestingMatches < this.maxNumShortMatches)
            maxNumInterestingMatches = this.maxNumShortMatches;
          if (maxNumInterestingMatches > 32766) {
            maxNumInterestingMatches = 32766; // must fit into a short, see PackedMap
          }
          if (maxNumInterestingMatches < 1)
            maxNumInterestingMatches = 1;
          blocksOfThisSize = new PackedMap(maxNumInterestingMatches, estimatedCapacity, this.sequenceDatabase, key1);
          while (this.hashedBlocks.size() <= key1)
            this.hashedBlocks.add(null);
          this.hashedBlocks.set(key1, blocksOfThisSize);
        }
      }
      // The reason we're building this database is so we can in the future ask for all of the places that a HashBlock can be found at
      // When we do that query, we don't want any location to appear more than once
      // If we simply put each HashBlock into the PackedMap, it's possible to get duplicate locations because a MultiHashBlock contains multiple ConditionalHashBlock and sometimes if we are unlucky they can even have the same start index, size, and hash code
      // So, if there are any MultiHashBlock here, we have to make sure to skip adding duplicates
      // We have to do the duplication check inside the PackedMap because the question of whether two HashBlocks count as duplicates could depend on which bin inside the PackedMap they belong in - even two HashBlock with different hashcode can potentially be put into the same bin and can subsequently both be returned from the same query, and if they encode ths same position, that would be confusing to the caller
      blocksOfThisSize.add(sequence, blocksHere, containsAmbiguousPosition);
    }
  }

  // Adds a HashBlock into a Map<length, List<HashBlock>>
  // Used by the previous function, addHashBlocks
  private void addToBlocksBySize(HashBlock block, Sequence sequence, Map<Integer, List<HashBlock>> blocksBySize) {
    if (this.enableGapmers)
      block = block.withGapAndExtension(sequence);
    if (block == null)
      return; // couldn't extend this block
    int length = block.getNumBasepairsUsed();
    if (length < this.minInterestingSize)
      return; // not interesting
    if (length <= this.maxFullySetUpSize)
      return; // already saved this block
    if (length > this.maxInterestingSize)
      return; // not interested in this block at the moment
    // We use the length as a key for a few reasons:
    // 1. It allows us to avoid putting so many blocks into one PackedMap (otherwise we reach Integer.MAX_VALUE entries in one map)
    // 2. It means when a caller does a lookup for a block, the caller is guaranteed that the block actually has the expected size, even if there is a hash collision. This means the caller doesn't have to do more length checks to be sure that the reference exists in that location
    // 3. It's convenient for supporting parallelism when building the database: each worker can lock one map at a time
    List<HashBlock> newBlocksWithThisSize = blocksBySize.get(length);
    if (newBlocksWithThisSize == null) {
      newBlocksWithThisSize = new ArrayList<HashBlock>();
      blocksBySize.put(length, newBlocksWithThisSize);
    }
    newBlocksWithThisSize.add(block);
  }


  // estimates the maximum capacity that could be required to hold all relevant HashBlocks of the given length, assuming that there aren't many duplicate hashblocks among them
  private int estimateRequiredCapacity(int numPositionsPerBlock) {
    int anchorBlockSize;
    if (this.getEnableGapmers())
      anchorBlockSize = numPositionsPerBlock * 2 / 3;
    else
      anchorBlockSize = numPositionsPerBlock;
    // the probability that a block from the nearest batch has this size
    double sizeProbability = Math.min(1, 2.0 / anchorBlockSize);

    // the probability that a block from the nearest batch has any particular offset
    double offsetProbability = Math.min(1, 2.0 / anchorBlockSize);

    // the probability that a block from the nearest batch has this size and any particular offset
    double blockPossibilityProbability = sizeProbability * offsetProbability;

    // the number of unique sequences of this length that can ever exist
    long maxNumSequencesOfThisLength;

    if (numPositionsPerBlock <= 16)
      maxNumSequencesOfThisLength = (1L << (numPositionsPerBlock * 2));
    else
      maxNumSequencesOfThisLength = (1L << 32);

    // We make sure that if we store a hashblock then we don't store its reverse complement
    long maxNumStoredSequencesOfThisLength = maxNumSequencesOfThisLength / 2;

    // the number of unique hashcodes for blocks of this size that can exist in any sequence
    long maxNumExistentHashcodes = (long)(maxNumStoredSequencesOfThisLength * blockPossibilityProbability);

    // estimate effective size including ambiguities
    long effectiveSize = this.totalForwardSize;

    // the number of blocks of this size that we might have including duplicates
    long numBlocksOfThisSize = (long)(effectiveSize * blockPossibilityProbability);

    // the number of blocks that do exist in our reference divided by the number that could exist in any sequence
    double existenceFraction = 1 - Math.pow((double)(maxNumExistentHashcodes - 1.0) / maxNumExistentHashcodes, numBlocksOfThisSize);

    // the number of unique blocks of this size
    int uniqueCount = (int)(maxNumExistentHashcodes * existenceFraction);

    int result = uniqueCount;
    if (result % 2 == 0)
      result++;
    return result;
  }

  public void updateThroughSize(Readable_HashBlock_Database toUpdate, int size) {
    this.requireSetUpThroughSize(size);
    synchronized(this) {
      toUpdate.update(this.hashedBlocks, this.maxFullySetUpSize);
    }
  }

  public int getMinInterestingSize() {
    return this.minInterestingSize;
  }

  public boolean getEnableGapmers() {
    return this.enableGapmers;
  }

  // Map<blockLength, Map<hashcode, List<HashBlock>>>
  List<PackedMap> hashedBlocks = new ArrayList<PackedMap>();

  // we've already hashed all possible blocks of this size or larger
  int maxFullySetUpSize = 0;
  int maxInterestingSize = 0;
  int minInterestingSize;

  // how many copies of short matches to store
  int maxNumShortMatches;

  long totalForwardSize;
  SequenceDatabase sequenceDatabase;
  Queue<Integer> lengthsLeftToLoad = new ArrayDeque<Integer>();
  Queue<Sequence> sequencesLeftToHash = new ArrayDeque<Sequence>();
  Queue<PackedMap> mapsLeftToPack = new ArrayDeque<PackedMap>();
  int numActiveLoaders;
  int numActiveHashers;
  int numActivePackers;
  int minNonloadableLength;
  Logger logger;
  File cacheDir;
  StatusLogger statusLogger;
  long cumulativeHashedSize;
  boolean enableGapmers = true;
}
