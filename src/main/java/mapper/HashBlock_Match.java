package mapper;

import java.util.List;

public class HashBlock_Match {
  public HashBlock_Match(HashBlock block, SequencePosition[] matches) {
    this.block = block;
    this.matches = matches;
  }
  public HashBlock getBlock() {
    return this.block;
  }
  public SequencePosition[] getMatches() {
    return this.matches;
  }

  public HashBlock block;
  public SequencePosition[] matches;
}
