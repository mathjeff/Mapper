package mapper;

import java.util.List;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Test;

public class HashBlock_Test {
  public HashBlock_Test() {
  }

  @Test
  public void testShortSymmetry() {
    checkSymmetry("A");
    checkSymmetry("C");
    checkSymmetry("G");
    checkSymmetry("T");
  }

  @Test
  public void testMediumSymmetry() {
    checkSymmetry("ACGTAACCGGTTACAGATCG");
  }

  @Test
  public void testLongSymmetry() {
    checkSymmetry("TGTGTATATATAGCAAGAAGTGTCCTTGTCGGACAATTCTTGCTTTTCTCGCTTTGCTCAAAAAGATTTTAAGATTACCTTTGTGGCATGGAACTAAGACGGAACGAAAAGATTACATTCCGGTGTACCGAACTTGAAAAGGACGCACTT");
  }

  private void checkSymmetry(String text) {
    Sequence sequence = new SequenceBuilder().setName("q").add(text).build();
    HashBlock_Stream stream = new HashBlock_Stream(sequence, true, null);
    while (true) {
      HashBlock_Row row = stream.getNextBatch();
      if (row == null)
        return;
      IMultiHashBlock firstBlock = row.getAfter(-1);
      if (firstBlock == null)
        return;
      int i = -1;
      while (true) {
        IMultiHashBlock block = row.getAfter(i);
        if (block == null)
          break;
        HashBlock single = block.getSingle();
        if (single == null)
          continue;
        checkBlockSymmetry(single, sequence);
        i = block.getStartIndex();
      }
    }
  }

  private void checkBlockSymmetry(HashBlock block, Sequence sequence) {
    Sequence reverseSequence = sequence.reverseComplement();
    HashBlock reverseBlock = hashSequence(reverseSequence, sequence.getLength() - block.getEndIndex(), sequence.getLength() - block.getStartIndex());
    if (reverseBlock.getForwardHash() != block.getReverseHash()) {
      Assert.fail("different hash: " + block.toString(sequence) + ", " + reverseBlock.toString(reverseSequence));
    }
    if (reverseBlock.getReverseHash() != block.getForwardHash()) {
      Assert.fail("different hash: " + block.toString(sequence) + ", " + reverseBlock.toString(reverseSequence));
    }
    if (block.get_requestMergeLeft() != reverseBlock.get_requestMergeRight()) {
      Assert.fail("different merge directions: " + block.toString(sequence) + " merge left = " + block.get_requestMergeLeft() + " and " + reverseBlock.toString(reverseSequence) + " merge right = " + reverseBlock.get_requestMergeRight());
    }
    if (block.get_requestMergeRight() != reverseBlock.get_requestMergeLeft()) {
      Assert.fail("different merge directions: " + block.toString(sequence) + " merge right = " + block.get_requestMergeRight() + " and " + reverseBlock.toString(reverseSequence) + " merge left = " + reverseBlock.get_requestMergeLeft());
    }
    if (block.get_nextRequestMergeLeft() != reverseBlock.get_nextRequestMergeRight()) {
      Assert.fail("different merge directions: " + block.toString(sequence) + " next merge left = " + block.get_nextRequestMergeLeft() + " and " + reverseBlock.toString(reverseSequence) + " next merge right = " + reverseBlock.get_nextRequestMergeRight());
    }
    if (block.get_nextRequestMergeRight() != reverseBlock.get_nextRequestMergeLeft()) {
      Assert.fail("different merge directions: " + block.toString(sequence) + " next merge right = " + block.get_nextRequestMergeRight() + " and " + reverseBlock.toString(reverseSequence) + " next merge left = " + reverseBlock.get_nextRequestMergeLeft());
    }
    if (!block.isPrimaryPolarity() && !block.isSecondaryPolarity()) {
      Assert.fail("Block " + block.toString(sequence) + " declares neither primary nor secondary polarity");
    }
    HashBlock extended = block.withGapAndExtension(sequence);
    HashBlock reverseExtended = reverseBlock.withGapAndExtension(reverseSequence);
    if ((extended == null) != (reverseExtended == null)) {
      Assert.fail("extension of " + block.toString(sequence) + " is " + extended + " but extension of " + reverseBlock.toString(reverseSequence) + " is " + reverseExtended);
    }
    if (extended == null) {
      return;
    }
    if (reverseExtended.getForwardHash() != extended.getReverseHash()) {
      Assert.fail("different hash: " + extended.toString(sequence) + ", " + reverseExtended.toString(reverseSequence));
    }
    if (reverseExtended.getReverseHash() != extended.getForwardHash()) {
      Assert.fail("different hash: " + extended.toString(sequence) + ", " + reverseExtended.toString(reverseSequence));
    }
  }

  private HashBlock hashSequence(Sequence sequence, int startIndex, int endIndex) {
    HashBlock_Stream stream = new HashBlock_Stream(sequence, true, null);
    while (true) {
      HashBlock_Row row = stream.getNextBatch();
      if (row == null)
        return null;
      IMultiHashBlock block = row.get(startIndex);
      if (block == null)
        return null;
      for (ConditionalHashBlock conditional : block.getPossibilities()) {
        HashBlock possibility = conditional.getHashBlock();
        if (possibility != null) {
          if (possibility.getEndIndex() == endIndex) {
            return possibility;
          }
        }
      }
    }
  }

}
