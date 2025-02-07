package mapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

// searches in the given hashblockDatabase for long, duplicated sections of genome
public class DuplicationDetector {
  // determine the minimum length of duplication that we're interested in
  public static int chooseMinDuplicationLength(SequenceDatabase reference) {
    int bitsToEncodeReferencePosition = reference.log2RoundUp(reference.getTotalForwardSize());
    // int basesToEncodeReferencePosition = bitsToEncodeReferencePosition / 2;
    // Choose the minimum number of basepairs that we must check to find 2 copies of before we consider them to be highly similar
    // If this value is too large, we might miss detecting some highly similar sections
    // If this value is too low, we might report too many sections as being similar
    // If we have one false positive detection per genome, that should probably be ok
    // A random hashblock using basesToEncodeReferencePosition * 2 should have, on average, 1/N matches
    // If every hashblock is random, N hashblocks of that length should have, in total, 1 matche
    // int minNumBasesUsed = basesToEncodeReferencePosition * 2;
    // int minDuplicationLength = basesToEncodeReferencePosition * 2;
    // int minDuplicationLength = bitsToEncodeReferencePosition;
    int minDuplicationLength = bitsToEncodeReferencePosition;
    return minDuplicationLength;
  }

  // determine the maximum length of duplication that we're interested in
  public static int chooseMaxDuplicationLength(SequenceDatabase sequenceDatabase) {
    return chooseMinDuplicationLength(sequenceDatabase) * 2;
  }

  public DuplicationDetector(ReferenceProvider hashblockDatabaseProvider, int minDuplicationLength, int maxDuplicationLength, int minNumInterestingCopies, int windowSize, DirCache dirCache, StatusLogger statusLogger) {
    this.setup(hashblockDatabaseProvider, minDuplicationLength, maxDuplicationLength, minNumInterestingCopies, windowSize, dirCache, statusLogger);
  }

  private void setup(ReferenceProvider hashblockDatabaseProvider, int minDuplicationLength, int maxDuplicationLength, int minNumInterestingCopies, int windowSize, DirCache dirCache, StatusLogger statusLogger) {
    this.dirCache = dirCache;
    this.hashblockDatabaseProvider = hashblockDatabaseProvider;
    this.enableGapmers = hashblockDatabaseProvider.getEnableGapmers();
    this.minSizeToProcess = minDuplicationLength;
    this.nextSizeToProcess = minDuplicationLength;
    this.maxSizeToProcess = maxDuplicationLength;
    this.numUncompleteJobs = this.maxSizeToProcess - this.nextSizeToProcess + 1;
    this.minNumInterestingCopies = minNumInterestingCopies;
    this.windowSize = windowSize;
    this.statusLogger = statusLogger;
  }

  // Returns a new view of this detector to allow usage from a separate thread
  // If multiple threads need access to this detector, each one should interact solely with its own view
  public Readable_DuplicationDetector getView(Logger logger) {
    return new Readable_DuplicationDetector(this, logger);
  }

  public void setup(Readable_DuplicationDetector view, Logger logger) {
    this.detect(logger);
    view.setup(this.duplicationsBySequence);
  }

  // tells the maximum average distance that can between consecutive mutations for us to not detect a duplication
  public double getDetectionGranularity() {
    if (this.enableGapmers) {
      // With gapmers that look like XX_X, we can't match two sequences when their pattern of mutations looks like ___!!
      // This pattern is 5 sections long and has 2 mutations
      // Each section is 1/4 of the length of the gapmer
      // So, this pattern is 5/4 the length of a gapmer and has 2 mutations
      // So, the average distance between mutations is 5/4/2=5/8 of a gapmer
      return this.minSizeToProcess * 5 / 8;
    }
    return this.minSizeToProcess;
  }

  public int getWindowSize() {
    return this.windowSize;
  }

  // must have called detect() first
  public Set<Duplication> getAll() {
    if (this.allDuplications == null) {
      synchronized(this) {
        Set<Duplication> all = new HashSet<Duplication>();
        for (TreeMap<Integer, Duplication> duplicationsHere: this.duplicationsBySequence.values()) {
          all.addAll(duplicationsHere.values());
        }
        this.allDuplications = all;
      }
    }
    return this.allDuplications;
  }

  private void detect(Logger logger) {
    while (!this.done()) {
      int nextSizeToProcess = this.getNextSizeToProcess();
      if (nextSizeToProcess >= 0) {
        this.process(nextSizeToProcess, logger);
      }
    }
  }

  private int getNextSizeToProcess() {
    // check whether we have already created our list of pending jobs
    synchronized(this) {
      if (this.nextSizeToProcess <= this.maxSizeToProcess) {
        int result = this.nextSizeToProcess;
        this.nextSizeToProcess++;
        return result;
      }
    }
    return -1;
  }

  private Readable_HashBlock_Database get_ReadableHashblockDatabase(Logger logger) {
    Readable_HashBlock_Database database = this.getHashblockDatabase(logger).getView();
    // Ensure a consistent hashed length even if we visit block lengths out of order
    database.ensureHashed(this.minSizeToProcess + 1);
    return database;
  }

  private HashBlock_Database getHashblockDatabase(Logger logger) {
    return this.hashblockDatabaseProvider.get_HashBlock_database(logger);
  }

  private void process(int blockLength, Logger logger) {
    this.statusLogger.log("DuplicationDetector starting to process length " + blockLength, false);
    HashBlock_Database hashblockDatabase = this.getHashblockDatabase(logger);
    Readable_HashBlock_Database readableHashblockDatabase = this.get_ReadableHashblockDatabase(logger);
    SequenceDatabase sequenceDatabase = hashblockDatabase.getSequenceDatabase();

    // Try to load from cache
    boolean loadedFromCache = false;
    File cacheDir = null;
    try {
      cacheDir = this.getCacheDir(hashblockDatabase);
    } catch (IOException e) {
      // If we were told to use the cache and can't, that's a fatal error
      throw new RuntimeException("DuplicationDetector cannot get cache dir", e);
    }
    if (cacheDir != null) {
      File cacheFile = this.getCacheFile(cacheDir, blockLength);
      if (cacheFile.exists()) {
        Map<Sequence, TreeMap<Integer, Duplication>> blocks = new HashMap<Sequence, TreeMap<Integer, Duplication>>();
        try {
          List<Duplication> duplications = this.readFromCache(cacheFile, blockLength, sequenceDatabase);
          // for each group, save it at the locations of each of its elements
          groupDuplicationsBySequence(duplications, blocks);
          saveDuplications(blocks);
          loadedFromCache = true;
         } catch (Exception e) {
           // Add the filepath to the exception
           throw new RuntimeException("Could not load duplications from " + cacheFile, e);
         }
      }
    }
   
    if (!loadedFromCache) { 
      this.cacheIncomplete = true; // we have new data to write to the cache
      // visit blocks of this size
      int numBlocks = readableHashblockDatabase.getNumHashKeys(blockLength);
      Map<Sequence, TreeMap<Integer, Duplication>> blocks = new HashMap<Sequence, TreeMap<Integer, Duplication>>();
      for (int hashcode = 0; hashcode < numBlocks; hashcode++) {
        if (hashcode % 1000 == 0) {
          this.statusLogger.log("DuplicationDetector starting to process hashcode " + hashcode + " / " + numBlocks + " for length " + blockLength, false);
        }
        SequencePosition[] matches = readableHashblockDatabase.lookupByForwardHash(blockLength, hashcode);
        if (matches == null) {
          // we have too many copies of this hashblock so we didn't save any of them
          continue;
        }
        // Check that this group might be big enough to be interesting.
        // When we do a lookup using the forward hash, we get the forward and reverse complement copy of each hashblock
        // So, the number of unique positions is half of that
        // It is possible that a hashblock is its own reverse complement, however, there aren't any cases at the moment where we're interested in that kind of duplication
        int numForwardMatches = matches.length / 2;
        if (numForwardMatches >= this.minNumInterestingCopies) {
          // group these blocks by part of their text to avoid hash collisions
          Map<String, Duplication> positionsByText = new HashMap<String, Duplication>();
          for (int i = 0; i < matches.length; i++) {
            SequencePosition position = matches[i];
            // We use just the edges of the block so we can still allow some mutations within a gapmer
            int prefixLength = (blockLength + 3) / 4;
            String prefix = position.getSequence().getRange(position.getStartIndex(), prefixLength);
            String suffix = position.getSequence().getRange(position.getStartIndex() + blockLength - prefixLength, prefixLength);
            String text = prefix + suffix;
            // ambiguous alleles could be reported via several different hashcodes, so for now we don't support detecting duplicate blocks containing ambiguous alleles
            if (!Basepairs.isAmbiguous(text)) {
              Duplication matchingPositions = positionsByText.get(text);
              if (matchingPositions == null) {
                matchingPositions = new Duplication(blockLength);
                positionsByText.put(text, matchingPositions);
              }
              matchingPositions.addPosition(position);
            }
          }
          // for each group, remove any positions that are listed twice
          for (Duplication group: positionsByText.values()) {
            group.removeDuplicatePositions();
          }
          // for each group, save it at the locations of each of its elements
          groupDuplicationsBySequence(positionsByText.values(), blocks);
        }
  
        // periodically copy results from blocks into this.duplicationsBySequence
        if (hashcode % 10000 == 9999 || hashcode == numBlocks - 1) {
          saveDuplications(blocks);
          blocks = new HashMap<Sequence, TreeMap<Integer, Duplication>>();
        }
      }
    }

    // record that we're done
    synchronized(this) {
      this.numUncompleteJobs--;
      if (this.numUncompleteJobs < 1) {
        // save to cache
        // TODO: format the duplications in parallel
        boolean savedToCache = false;
        if (cacheDir != null && this.cacheIncomplete) {
          this.saveToCache(cacheDir, sequenceDatabase);
          savedToCache = true;
        }

        // don't need the HashBlock_Database anymore
        this.hashblockDatabaseProvider = null;

        // Check whether we need to keep the Duplications
        if (this.windowSize > 1) {
          // Clear the Duplications
          for (Map<Integer, Duplication> entrySetHere: this.duplicationsBySequence.values()) {
            for (Map.Entry<Integer, Duplication> entry: entrySetHere.entrySet()) {
              entry.setValue(null);
            }
          }
          this.allDuplications = null;
        }

        if (savedToCache) {
          this.statusLogger.log("DuplicationDetector done detecting duplications (through length " + this.maxSizeToProcess + ")", true);
        } else {
          // Nothing was saved to the cache so we only loaded from the cache
          this.statusLogger.log("DuplicationDetector done loading duplications (through length " + this.maxSizeToProcess + ")", true);
        }
      }
    }
  }

  private void groupDuplicationsBySequence(Iterable<Duplication> duplications, Map<Sequence, TreeMap<Integer, Duplication>> blocks) {
    // for each group, save it at the locations of each of its elements
    for (Duplication group: duplications) {
      if (group.getNumInstances() >= this.minNumInterestingCopies) {
        for (SequencePosition position: group.getStartPositions()) {
          // associate this group with this position
          Sequence sequence = position.getSequence();
          TreeMap<Integer, Duplication> blocksOnThisSequence = blocks.get(sequence);
          if (blocksOnThisSequence == null) {
            blocksOnThisSequence = new TreeMap<Integer, Duplication>();
            blocks.put(sequence, blocksOnThisSequence);
          }
          int startIndex = position.getStartIndex();
          blocksOnThisSequence.put(startIndex, group);
        }
      }
    }
  }

  private List<Set<Duplication>> getDuplicationsByLength() {
    List<Set<Duplication>> duplicationsByLength = new ArrayList<Set<Duplication>>();
    for (int i = 0; i <= this.maxSizeToProcess; i++) {
      duplicationsByLength.add(new HashSet<Duplication>());
    }

    for (TreeMap<Integer, Duplication> duplicationsHere: this.duplicationsBySequence.values()) {
      for (Duplication duplication: duplicationsHere.values()) {
        int length = duplication.getLength();
        duplicationsByLength.get(length).add(duplication);
      }
    }
    return duplicationsByLength;
  }

  private void saveToCache(File cacheDir, SequenceDatabase sequenceDatabase) {
    this.statusLogger.log("DuplicationDetector saving to cache", true);
    try {
      List<Set<Duplication>> duplicationsByLength = getDuplicationsByLength();
      for (int length = this.minSizeToProcess; length <= this.maxSizeToProcess; length++) {
        Set<Duplication> duplicationsWithThisLength = duplicationsByLength.get(length);
        File cacheFile = getCacheFile(cacheDir, length);
        this.writeToCache(duplicationsWithThisLength, cacheFile, sequenceDatabase);
      }
    } catch (IOException e) {
      // If we were told to save to a cache directory and can't, that's a fatal error
      throw new RuntimeException("DuplicationDetector could not save to cache dir " + cacheDir, e);
    }
    this.statusLogger.log("DuplicationDetector saved to cache " + cacheDir, true);
  }

  private void writeToCache(Set<Duplication> duplications, File cacheFile, SequenceDatabase sequenceDatabase) throws IOException {
    Serializer serializer = new Serializer(cacheFile);
    serializer.writeProperty("numDuplications", "" + duplications.size());
    for (Duplication duplication: duplications) {
      serializer.writeString("" + duplication.getNumInstances() + ":");
      for (SequencePosition position: duplication.getStartPositions()) {
        long encodedPosition = sequenceDatabase.encodePosition(position.getSequence(), position.getStartIndex());
        serializer.writeString("" + encodedPosition + ",");
      }
    }
    serializer.close();
  }

  private List<Duplication> readFromCache(File cacheFile, int duplicationLength, SequenceDatabase sequenceDatabase) throws IOException {
    Deserializer deserializer = new Deserializer(cacheFile);
    int numDuplications = deserializer.readIntProperty("numDuplications");
    List<Duplication> duplications = new ArrayList<Duplication>();
    for (int i = 0; i < numDuplications; i++) {
      int numInstances = Integer.parseInt(deserializer.readUntil(':'));
      Duplication duplication = new Duplication(duplicationLength);
      for (int j = 0; j < numInstances; j++) {
        long encodedPosition = Long.parseLong(deserializer.readUntil(','));
        SequencePosition position = sequenceDatabase.decodePosition(encodedPosition);
        duplication.addPosition(position);
      }
      duplications.add(duplication);
    }
    return duplications;
  }

  private void saveDuplications(Map<Sequence, TreeMap<Integer, Duplication>> blocks) {
    for (Map.Entry<Sequence, TreeMap<Integer, Duplication>> entry: blocks.entrySet()) {
      Sequence sequence = entry.getKey();
      TreeMap<Integer, Duplication> currentPositionsOnThisSequence = entry.getValue();
      TreeMap<Integer, Duplication> allPositionsOnThisSequence;
      synchronized(this) {
        allPositionsOnThisSequence = this.duplicationsBySequence.get(sequence);
        if (allPositionsOnThisSequence == null) {
          allPositionsOnThisSequence = new TreeMap<Integer, Duplication>();
          this.duplicationsBySequence.put(sequence, allPositionsOnThisSequence);
        }
      }
      synchronized(allPositionsOnThisSequence) {
        for (Map.Entry<Integer, Duplication> positions: currentPositionsOnThisSequence.entrySet()) {
          // allPositionsOnThisSequence always contains the set of Duplications such that each such Duplication is not contained by any other Duplication
          // When discover a new Duplication to add, we might have to skip it or remove a neighbor

          int duplicationStart = positions.getKey();
          Duplication newDuplication = positions.getValue();
          int duplicationEnd = duplicationStart + newDuplication.getLength();

          // remove each duplication that spans this one (starting as early and ending as late)
          boolean insert = true;
          while (true) {
            Map.Entry<Integer, Duplication> existingLowerGroup = allPositionsOnThisSequence.floorEntry(duplicationStart);
            if (existingLowerGroup != null) {
              int comparison = compareDuplications(duplicationStart, newDuplication, existingLowerGroup.getKey(), existingLowerGroup.getValue());
              if (comparison > 0) {
                // We already have an better entry for this duplication; don't need to insert this one
                insert = false;
                break;
              }
              if (comparison < 0) {
                // This duplication is better than the previously detected duplication
                allPositionsOnThisSequence.remove(existingLowerGroup.getKey());
                // look for other, worse duplications
                continue;
              }
            }
            // didn't find any other worse duplications
            break;
          }

          while (true) {
            Map.Entry<Integer, Duplication> existingHigherGroup = allPositionsOnThisSequence.ceilingEntry(duplicationStart);
            if (existingHigherGroup != null) {
              int comparison = compareDuplications(duplicationStart, newDuplication, existingHigherGroup.getKey(), existingHigherGroup.getValue());

              if (comparison > 0) {
                // We already have an entry for this duplication
                insert = false;
                break;
              }
              if (comparison < 0) {
                // This duplication is better than the previously detected duplication
                allPositionsOnThisSequence.remove(existingHigherGroup.getKey());
                // look for other, worse duplications
                continue;
              }
            }
            break;
          }

          if (insert)
            allPositionsOnThisSequence.put(duplicationStart, newDuplication);
        }
      }
    }
  }

  // Compares two duplications to determine which one is better to keep
  // A positive number means duplication2 is better
  // A negative number means duplication1 is better
  // A negative number means they're independent
  private int compareDuplications(int start1, Duplication duplication1, int start2, Duplication duplication2) {
    if (this.windowSize > 1) {
      // If we've been asked to use a certain window size, then duplications in different windows aren't comparable
      int window1 = getWindowNumber(start1);
      int window2 = getWindowNumber(start2);
      if (window1 != window2) {
        return 0;
      }
    }

    // If one duplication contains another, prefer the contained one
    int end1 = start1 + duplication1.getLength();
    int end2 = start2 + duplication2.getLength();
    if (start1 <= start2 && end1 >= end2) {
      return 1; // duplication2 is more specific which is better
    }
    if (start1 >= start2 && end1 <= end2) {
      return -1; // duplication1 is more specific which is better
    }

    if (this.windowSize > 1) {
      // Within a certain window, prefer to keep the Duplication having more copies, because that shouldn't require retaining as many objects
      int countDifference = duplication1.getNumInstances() - duplication2.getNumInstances();
      if (countDifference != 0)
        return countDifference;
      // Break ties based on start position
      if (start1 != start2)
        return start1 - start2;
    }
    return 0;
  }

  public int getWindowNumber(int index) {
    return index / this.windowSize;
  }

  private boolean done() {
    synchronized(this) {
      return this.numUncompleteJobs < 1;
    }
  }

  private File getCacheDir(HashBlock_Database database) throws IOException {
    if (this.dirCache == null) {
      // We aren't supposed to use a cache directory
      return null;
    }
    if (this.cacheDir == null) {
      // We haven't yet computed a cache directory
      TreeMap<String, String> keys = new TreeMap<String, String>(database.getCacheKeys());
      keys.put("minSizeToProcess", "" + this.minSizeToProcess);
      keys.put("maxSizeToProcess", "" + this.maxSizeToProcess);
      keys.put("windowSize", "" + this.windowSize);
      keys.put("minNumInterestingCopies", "" + this.minNumInterestingCopies);
      keys.put("type", "DuplicationDetector");

      this.cacheDir = dirCache.getOrCreateDir(keys);
    }
    return this.cacheDir;
  }

  private File getCacheFile(File cacheDir, int duplicationLength) {
    return new File(cacheDir, "length-" + duplicationLength);
  }

  ReferenceProvider hashblockDatabaseProvider;
  boolean enableGapmers;
  Readable_DuplicationDetector detected;
  Map<Sequence, TreeMap<Integer, Duplication>> duplicationsBySequence = new HashMap<Sequence, TreeMap<Integer, Duplication>>();
  Set<Duplication> allDuplications;
  int minSizeToProcess;
  int nextSizeToProcess;
  int maxSizeToProcess;
  int numUncompleteJobs;
  int minNumInterestingCopies;
  int windowSize; // We group duplications into windows of this size and only keep the min and max in each window
  DirCache dirCache;
  File cacheDir;
  StatusLogger statusLogger;
  boolean cacheIncomplete = false;
}
