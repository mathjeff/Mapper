package mapper;

import java.util.ArrayList;
import java.util.List;

public class Gapped_HashBlock extends HashBlock {
  public Gapped_HashBlock(int block1Start, int block1Length, int gapLength, int block2Length) {
    super(block1Start, block1Length + gapLength + block2Length);
    this.block1Length = block1Length;
    this.gapLength = gapLength;
  }

  @Override
  public String getText(Sequence sequence) {
    String prefix = sequence.getRange(this.getStartIndex(), this.block1Length);
    String gap = repeat("_", this.gapLength);
    String suffix = sequence.getRange(this.getStartIndex() + this.block1Length + this.gapLength, this.getSuffixLength());
    return prefix + gap + suffix;
  }

  private String repeat(String text, int count) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < count; i++) {
      builder.append(text);
    }
    return builder.toString();
  }

  private int getSuffixLength() {
    return this.getLength() - this.block1Length - this.gapLength;
  }

  private int block1Length;
  private int gapLength;
}
