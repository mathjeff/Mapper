package mapper;

import java.util.List;

// a HashJob specifies that a certain part of the reference genome needs to be analyzed to generate HashBlocks
public class HashJob {
  public HashJob(Sequence sequence, int minStartIndex, int maxStartIndexExclusive) {
    this.sequence = sequence;
    this.minStartIndex = minStartIndex;
    this.maxStartIndexExclusive = maxStartIndexExclusive;
  }

  public Sequence sequence;
  public int minStartIndex;
  public int maxStartIndexExclusive;
}
