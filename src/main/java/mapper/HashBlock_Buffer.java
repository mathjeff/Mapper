package mapper;

import java.util.ArrayList;
import java.util.List;

// a HashBlock_Buffer listens for hashblocks, stores them, and emits a lot of them at once to a HashBlock_Database
public class HashBlock_Buffer {
  public HashBlock_Buffer(Sequence sequence, HashBlock_Database database, int minInterestingSize) {
    this.sequence = sequence;
    this.database = database;
    this.minInterestingSize = minInterestingSize;
  }

  public void addHashblock(IMultiHashBlock block) {
    HashBlock single = block.getSingle();

    if (single == null) {
      this.multiBlocks.add(block);
      if (this.multiBlocks.size() >= 65536) {
        this.flush();
      }
    } else {
      this.singleBlocks.add(block);
      if (this.singleBlocks.size() >= 8096) {
        this.flush();
       }
    }
  }

  public void flush() {
    this.database.addHashblocks(this.sequence, this.multiBlocks);
    this.multiBlocks.clear();
    this.database.addHashblocks(this.sequence, this.singleBlocks);
    this.singleBlocks.clear();
  }

  public int getMinInterestingSize() {
    return this.minInterestingSize;
  }

  private List<IMultiHashBlock> multiBlocks = new ArrayList<IMultiHashBlock>();
  private List<IMultiHashBlock> singleBlocks = new ArrayList<IMultiHashBlock>();
  private HashBlock_Database database;
  private Sequence sequence;
  private int minInterestingSize; // we don't have to save any hashblocks shorter than this because the HashBlock_Database won't be interested in them
}
