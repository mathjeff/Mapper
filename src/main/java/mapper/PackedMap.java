package mapper;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// A PackedMap is a Map that uses less memory than a HashMap due to allocating fewer objects
class PackedMap {
  private static SequencePosition[] emptyList = new SequencePosition[0];

  public PackedMap(int maxInterestingCountPerKey, int keyCapacity, SequenceDatabase sequenceDatabase) {
    if (keyCapacity < 1)
      keyCapacity = 1;

    // It is possible that all of our values will be stored in the same array
    // We limit our maximum number of items stored per key to be no more than what we can be sure will fit in the array
    long numBytesPerPositionReference = sequenceDatabase.getEncodedLength(1);
    long maxArrayLength = Integer.MAX_VALUE / 2;
    if ((long)keyCapacity > maxArrayLength) {
      keyCapacity = (int)maxArrayLength;
    }

    this.maxInterestingCountPerKey = maxInterestingCountPerKey;
    if ((short)(this.maxInterestingCountPerKey + 1) != this.maxInterestingCountPerKey + 1) {
      throw new IllegalArgumentException("maxInterestingCountPerKey too large: " + this.maxInterestingCountPerKey + "; must fit in a short");
    }
    this.subArrayOffsets = new int[keyCapacity];
    if (maxInterestingCountPerKey <= 126) {
      // a byte can hold up to 127 without complicated calculations
      // We need to be able to store 1 more than the maximum interesting count per key so we can record when we have too many
      this.countsAsBytes = new byte[keyCapacity];
    } else {
      this.countsAsShorts = new short[keyCapacity];
    }

    this.sequenceDatabase = sequenceDatabase;
  }

  // Adds the given hashblocks to this map
  // If called from multiple threads, these blocks might not be added until after all threads have returned
  public void add(Sequence sequence, List<HashBlock> blocks, boolean preventDuplicates) {
    PackJob job = new PackJob(sequence, blocks, preventDuplicates);
    synchronized(this.pendingAdds) {
      this.pendingAdds.add(job);
      if (this.activelyAdding) {
        // there's already another thread processing PackJobs in this map
        return;
      }
      this.activelyAdding = true;
    }
    // If we're the first thread trying to add blocks to this map, then we have to do the activelyAdding
    processAll();
  }

  // processes all jobs in this.pendingAdds until none remain unprocessed
  private void processAll() {
    while (true) {
      PackJob job = null;
      synchronized(this.pendingAdds) {
        if (this.pendingAdds.size() > 0) {
          if (this.pendingAdds.size() > 32) {
            // A HashBlock uses more memory than its packed form so we can't have too many pending HashBlocks at once
            // If we get too many pending jobs, we block new jobs until existing jobs are done
            for (PackJob j : this.pendingAdds) {
              this.process(j);
            }
            this.pendingAdds.clear();
            this.activelyAdding = false;
            return;
          }

          // Normally we just select the next job to process and then reopen the queue
          job = this.pendingAdds.get(this.pendingAdds.size() - 1);
          this.pendingAdds.remove(this.pendingAdds.size() - 1);
        } else {
          // done
          this.activelyAdding = false;
          return;
        }
      }
      this.process(job);
    }
  }

  // adds each block in packJob
  private void process(PackJob packJob) {
    Sequence sequence = packJob.sequence;
    Sequence reverseSequence = this.sequenceDatabase.getReverseComplement(sequence);
    long addStartMillis = System.currentTimeMillis();
    for (HashBlock block : packJob.blocks) {
      Sequence blockSequence;
      int blockStart;
      int blockHash;
      if (block.isPrimaryPolarity()) {
        blockSequence = sequence;
        blockStart = block.getStartIndex();
        blockHash = block.getForwardHash();
        this.add(blockHash, blockSequence, blockStart, packJob.preventDuplicates);
      }
      if (block.isSecondaryPolarity()) {
        blockSequence = reverseSequence;
        blockStart = reverseSequence.getLength() - block.getEndIndex();
        blockHash = block.getReverseHash();
        this.add(blockHash, blockSequence, blockStart, packJob.preventDuplicates);
      }
    }
    long endMillis = System.currentTimeMillis();
    this.totalAddMillis += (endMillis - addStartMillis);
  }

  private void add(int key, Sequence sequence, int startIndex, boolean preventDuplicates) {
    // find where the existing data is
    int packedKey = getPackedKey(key);
    int existingNumItems = this.getCount(packedKey);

    this.numItemsAdded++;
    if (existingNumItems >= this.maxInterestingCountPerKey) {
      this.totalOverfill++;
      if (existingNumItems == this.maxInterestingCountPerKey) {
        this.numOverfilledKeys++;
        this.totalOverfill += existingNumItems;
      } else {
        // already have enough copies, don't need to store more
        return;
      }
    }

    if (preventDuplicates) {
      SequencePosition position = new SequencePosition(sequence, startIndex);
      SequencePosition[] existing = this.get(key, Integer.MAX_VALUE);
      if (existing != null) {
        for (int i = 0; i < existing.length; i++) {
          if (existing[i].equals(position)) {
            return; // duplicate
          }
        }
      }
    }

    byte[] newContent;
    // update some stats
    int newNumItems = existingNumItems + 1;
    this.setCount(packedKey, newNumItems);

    ByteBlockStore newRow = this.getOrCreateStore(newNumItems);
    int binNumber = this.subArrayOffsets[packedKey];

    if (existingNumItems > 0) {
      // find the existing data
      ByteBlockStore existingRow = this.getOrCreateStore(existingNumItems);
      if (newRow != existingRow) {
        // There isn't enough capacity to expand in place, so we'll have to move to a new block
        byte[] existingContents = existingRow.get(binNumber);
        existingRow.clear(binNumber);

        // Make sure the content is still short enough to be worth storing
        if (newNumItems > this.maxInterestingCountPerKey) {
          return;
        }

        binNumber = this.subArrayOffsets[packedKey] = newRow.put(existingContents);
      }
    } else {
      this.numUniqueKeysUsed++;
      // allocate a position for this new item
      binNumber = this.subArrayOffsets[packedKey] = newRow.put(new byte[0]);
    }

    // Now expand in place
    long newEncoded = this.sequenceDatabase.encodePosition(sequence, startIndex);
    this.subArrayOffsets[packedKey] = this.sequenceDatabase.writeEncodedPosition(newRow, binNumber, existingNumItems, newEncoded);

  }
  public int getMaxInterestingCountPerKey() {
    return this.maxInterestingCountPerKey;
  }
  public SequencePosition[] get(int key) {
    return this.get(key, Integer.MAX_VALUE);
  }
  public SequencePosition[] get(int key, int maxInterestingCount) {
    // find where the packed data is
    int packedKey = this.getPackedKey(key);
    int count = this.getCount(packedKey);
    if (count < 1)
      return emptyList;
    if (count > this.maxInterestingCountPerKey || count > maxInterestingCount) {
      return null; // too many matches
    }

    int numBytes = this.sequenceDatabase.getEncodedLength(count);
    ByteBlockStore store = this.getStore(count);
    int binNumber = this.subArrayOffsets[packedKey];

    // unpack
    return this.sequenceDatabase.unpackPositions(store, binNumber, numBytes);
  }

  /*private void sort(SequencePosition[] positions) {
    if (positions == null || positions.length < 2)
      return;
    int delta = positions.length / 2;
    while (true) {
      boolean swapped = false;
      for (int i = positions.length - delta - 1; i >= 0; i--) {
        int index2 = i + delta;
        SequencePosition a = positions[i];
        SequencePosition b = positions[index2];
        if (a.compareTo(b) > 0) {
          positions[i] = b;
          positions[index2] = a;
          swapped = true;
        }
      }
      if (delta > 1) {
        delta /= 2;
      } else {
        if (!swapped) {
          return;
        }
      }
    }
  }*/

  public boolean knowsAllMatches(int key) {
    int packedKey = getPackedKey(key);
    return this.getCount(packedKey) <= this.maxInterestingCountPerKey;
  }

  private ByteBlockStore getStore(int numItems) {
    int capacity = this.getBinCapacity(numItems);
    return this.data.get(capacity);
  }

  private ByteBlockStore getOrCreateStore(int numItems) {
    if (numItems > this.maxInterestingCountPerKey)
      return null;
    int capacity = this.getBinCapacity(numItems);
    while (this.data.size() <= capacity) {
      this.data.add(null);
    }
    ByteBlockStore result = this.data.get(capacity);
    if (result == null) {
      int numBytesPerBlock = this.sequenceDatabase.getEncodedLength(capacity);
      int keyCapacity = this.getCapacity();
      this.data.set(capacity, ByteBlockStore.create(keyCapacity, numBytesPerBlock));
      result = this.data.get(capacity);
    }
    return result;
  }

  // given a certain number of items that we want to store, specifies how much capacity we want in the corresponding bin
  private int getBinCapacity(int numItems) {
    int capacity = 1;
    while (capacity < numItems) {
      capacity = capacity * 11 / 10 + 1;
    }
    return capacity;
  }

  public int getCapacity() {
    return this.subArrayOffsets.length;
  }

  public int getNumUniqueKeysUsed() {
    return numUniqueKeysUsed;
  }

  public int getNumOverfilledKeys() {
    return numOverfilledKeys;
  }

  public long getNumItemsAdded() {
    return numItemsAdded;
  }

  public long getTotalOverfill() {
    return this.totalOverfill;
  }

  public long getTotalAddMillis() {
    return this.totalAddMillis;
  }

  private int getPackedKey(int originalKey) {
    int capacity = this.getCapacity();
    int modded = originalKey % capacity;
    if (modded < 0)
      modded += capacity;
    return modded;
  }

  public int getNumMatches(HashBlock block) {
    int key = getPackedKey(block.getLookupKey());
    int count = this.getCount(key);
    if (count > this.maxInterestingCountPerKey)
      return Integer.MAX_VALUE;
    return count;
  }

  // Orders contents deterministically, to be independent of the order it was inserted in
  public void orderDeterministically() {
    for (int hashcode = 0; hashcode < this.getCapacity(); hashcode++) {
      int count = getCount(hashcode);
      if (count < 2)
        continue;
      SequencePosition[] positions = this.get(hashcode, Integer.MAX_VALUE);
      if (positions == null) {
        // too many positions to store
        continue;
      }

      SequencePosition[] orderedPositions = OrderingUtils.orderDeterministically(positions);
      int binNumber = this.subArrayOffsets[hashcode];
      ByteBlockStore store = getStore(count);
      for (int itemIndex = 0; itemIndex < count; itemIndex++) {
        SequencePosition position = orderedPositions[itemIndex];
        long encoded = this.sequenceDatabase.encodePosition(position.getSequence(), position.getStartIndex());
        binNumber = this.sequenceDatabase.writeEncodedPosition(store, binNumber, itemIndex, encoded);
      }
      this.subArrayOffsets[hashcode] = binNumber;
    }
  }

  private int getCount(int index) {
    if (this.countsAsBytes != null)
      return this.countsAsBytes[index];
    return this.countsAsShorts[index];
  }

  private void setCount(int index, int value) {
    if (this.countsAsBytes != null) {
      this.countsAsBytes[index] = (byte)value;
    } else {
      this.countsAsShorts[index] = (short)value;
    }
  }
 
  // We represent a Map<hashCode, List<SequencePosition>>
  // We pack each individual list into a single Array to reduce the number of objects and save space

  // The array containing the data
  // The first index is the length of the subarray, referred to by this.countsAsShorts
  // The second index is a position referred to by subArrayOffsets
  List<ByteBlockStore> data = new ArrayList<ByteBlockStore>();

  // Array that tells where in the data array we can find individual subarrays
  // Index is the hashcode; value is the index in the data array
  int[] subArrayOffsets;

  // Array that tells how many items there are for a given hashcode
  // Index is the hashcode; value is the number items
  short[] countsAsShorts;
  // If the number of bytes required is small, we use countsAsBytes instead of countsAsShorts
  byte[] countsAsBytes;

  int numUniqueKeysUsed;
  int numOverfilledKeys;
  long totalOverfill;
  SequenceDatabase sequenceDatabase;
  long numItemsAdded;
  int maxInterestingByteCount;
  int maxInterestingCountPerKey;
  long totalAddMillis;

  List<PackJob> pendingAdds = new ArrayList<PackJob>();
  boolean activelyAdding = false;
}
