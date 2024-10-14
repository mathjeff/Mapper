package mapper;

import java.util.List;

// a PackJob specifies that there are some HashBlocks to add to a PackedMap
public class PackJob {
  public PackJob(Sequence sequence, List<HashBlock> blocks, boolean preventDuplicates) {
    this.sequence = sequence;
    this.blocks = blocks;
    this.preventDuplicates = preventDuplicates;
  }

  public Sequence sequence;
  public List<HashBlock> blocks;
  public boolean preventDuplicates;
}
