package mapper;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// A PackedMap is a Map that uses less memory than a HashMap due to allocating fewer objects
class PackedMap {
  private static SequencePosition[] emptyList = new SequencePosition[0];
  private static int keysPerStore = 256;

  public PackedMap(int maxInterestingCountPerKey, int keyCapacity, SequenceDatabase sequenceDatabase, int id) {
    if (keyCapacity < 1)
      keyCapacity = 1;
    long maxArrayLength = Integer.MAX_VALUE / 2;
    if ((long)keyCapacity > maxArrayLength) {
      keyCapacity = (int)maxArrayLength;
    }
    this.maxInterestingCountPerKey = maxInterestingCountPerKey;
    this.keyCapacity = keyCapacity;
    this.sequenceDatabase = sequenceDatabase;
    this.id = id;

    this.allocateStores();
  }

  public PackedMap(File fromCacheFile, SequenceDatabase sequenceDatabase) throws IOException {
    this.sequenceDatabase = sequenceDatabase;
    this.readFrom(fromCacheFile);
  }

  private void allocateStores() {
    // It is possible that all of our values will be stored in the same array
    // We limit our maximum number of items stored per key to be no more than what we can be sure will fit in the array
    int numBytesPerPositionReference = sequenceDatabase.getEncodedLength(1);
    int numBitsPerPositionReference = sequenceDatabase.getNumBitsPerPosition();

    int maxInterestingBytesPerKey = sequenceDatabase.getEncodedLength(maxInterestingCountPerKey);
    this.stores = new ByteKeyStore[(keyCapacity + keysPerStore - 1) / keysPerStore];
    for (int i = 0; i < stores.length; i++) {
      this.stores[i] = new ByteKeyStore(keysPerStore, maxInterestingBytesPerKey, numBitsPerPositionReference);
    }
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
    // make sure this data isn't already there
    boolean duplicate = false;
    if (preventDuplicates) {
      SequencePosition position = new SequencePosition(sequence, startIndex);
      SequencePosition[] existing = this.get(key, Integer.MAX_VALUE);
      if (existing != null) {
        for (int i = 0; i < existing.length; i++) {
          if (existing[i].equals(position)) {
            duplicate = true;
            break;
          }
        }
      }
    }
    boolean added = false;
    if (!duplicate) {
      // find where the existing data is
      int packedKey = getPackedKey(key);
      int indexOfStore = getIndexOfStore(packedKey);
      int indexInStore = getIndexInStore(packedKey);
      // convert to bytes and add
      ByteKeyStore store = this.stores[indexOfStore];
      long encoded = this.sequenceDatabase.encodePosition(sequence, startIndex);
      this.sequenceDatabase.appendEncodedPosition(store, indexInStore, encoded);
    }

    // update some statistics
    this.numItemsAdded++;
  }
  public int getMaxInterestingCountPerKey() {
    return this.maxInterestingCountPerKey;
  }
  public SequencePosition[] get(int key) {
    return this.get(key, Integer.MAX_VALUE);
  }
  public SequencePosition[] get(int key, int maxInterestingCount) {
    int count = this.getNumMatchesLowerBound(key);
    if (count > maxInterestingCount || count > this.maxInterestingCountPerKey)
      return null; // too many matches

    // Get the byte store
    int packedKey = getPackedKey(key);
    int indexOfStore = getIndexOfStore(packedKey);
    int indexInStore = getIndexInStore(packedKey);
    ByteKeyStore store = this.stores[indexOfStore];
    // unpack
    return this.sequenceDatabase.unpackPositions(store, indexInStore);
  }

  public boolean knowsAllMatches(int key) {
    int packedKey = getPackedKey(key);
    int indexOfStore = getIndexOfStore(packedKey);
    int indexInStore = getIndexInStore(packedKey);

    return getByteStore(indexOfStore).knowsAllMatches(indexInStore);
  }

  private ByteKeyStore getByteStore(int packedKey) {
    return this.stores[this.getIndexOfStore(packedKey)];
  }

  public int getCapacity() {
    return this.keyCapacity;
  }

  public int getNumOverfilledKeys() {
    int numOverfilledKeys = 0;
    for (int packedKey = 0; packedKey < this.keyCapacity; packedKey++) {
      int indexOfStore = getIndexOfStore(packedKey);
      int indexInStore = getIndexInStore(packedKey);
      if (!this.stores[indexOfStore].knowsAllMatches(indexInStore)) {
        numOverfilledKeys++;
      }
    }
    return numOverfilledKeys;
  }

  public long getNumItemsAdded() {
    return numItemsAdded;
  }

  public long getTotalAddMillis() {
    return this.totalAddMillis;
  }

  private int getPackedKey(int originalKey) {
    int result = originalKey % getCapacity();
    if (result < 0)
      result += this.getCapacity();
    return result;
  }

  private int getIndexOfStore(int packedKey) {
    return packedKey / keysPerStore;
  }

  private int getIndexInStore(int packedKey) {
    return packedKey % keysPerStore;
  }

  public int getNumMatchesLowerBound(HashBlock block) {
    return this.getNumMatchesLowerBound(block.getLookupKey());
  }
  public int getNumMatchesLowerBound(int key) {
    int packedKey = getPackedKey(key);
    int indexOfStore = getIndexOfStore(packedKey);
    int indexInStore = getIndexInStore(packedKey);
    ByteKeyStore store = this.stores[indexOfStore];
    if (!store.knowsAllMatches(indexInStore))
      return Integer.MAX_VALUE;
    return store.getNumValues(indexInStore);
  }

  // Orders contents deterministically, to be independent of the order it was inserted in
  public void pack() {
    for (int i = 0; i < this.stores.length; i++) {
      this.stores[i].pack();
    }
  }

  public int getId() {
    return this.id;
  }

  public void writeTo(File file) throws IOException {
    Serializer serializer = new Serializer(file);
    serializer.writeProperty("type", "PackedMap");
    serializer.writeProperty("keyCapacity", "" + this.keyCapacity);
    serializer.writeProperty("numItems", "" + this.numItemsAdded);
    serializer.writeProperty("maxCountPerKey", "" + this.maxInterestingCountPerKey);
    serializer.writeProperty("basepairs", "" + this.id);
    serializer.writeProperty("numStores", "" + this.stores.length);
    serializer.writeString("stores:");
    for (int i = 0; i < stores.length; i++) {
      stores[i].writeTo(serializer);
    }
    serializer.close();
  }


  public void readFrom(File file) throws IOException {
    Deserializer deserializer = new Deserializer(file);
    deserializer.readText("type:PackedMap,");
    this.keyCapacity = deserializer.readIntProperty("keyCapacity");
    this.numItemsAdded = deserializer.readIntProperty("numItems");
    this.maxInterestingCountPerKey = deserializer.readIntProperty("maxCountPerKey");
    this.id = deserializer.readIntProperty("basepairs");
    int numStores = deserializer.readIntProperty("numStores");
    deserializer.readText("stores:");
    this.allocateStores();
    for (int i = 0; i < this.stores.length; i++) {
      this.stores[i].readFrom(deserializer);
    }
    deserializer.close();
  }

  ByteKeyStore[] stores;
  int keyCapacity;

  SequenceDatabase sequenceDatabase;
  long numItemsAdded;
  int maxInterestingCountPerKey;
  long totalAddMillis;
  int id;

  List<PackJob> pendingAdds = new ArrayList<PackJob>();
  boolean activelyAdding = false;
}
