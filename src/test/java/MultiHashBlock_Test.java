package mapper;

import java.util.List;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Test;

public class MultiHashBlock_Test {
  public MultiHashBlock_Test() {
  }

  @Test
  public void testShortAmbiguities() {
    checkExpandingAmbiguities("A", 1);
  }

  @Test
  public void testMediumAmbiguities() {
    checkExpandingAmbiguities("AAA", 3);
  }

  @Test
  public void testLongAmbiguity() {
    checkExpandingAmbiguities("AAAAAAAAAAAAAAA", 3);
  }

  @Test
  public void testNonUniformAmbiguity() {
    checkExpandingAmbiguities("TTATGC", 1);
  }

  // This function tests that we expand each ambiguous letter into the appropriate specific basepairs
  @Test
  public void checkPartialAmbiguity() {
    // R
    checkExpandingAmbiguitiesInto("AAA", "ARA");
    checkExpandingAmbiguitiesInto("GGG", "GRG");
    // Y
    checkExpandingAmbiguitiesInto("CCC", "CYC");
    checkExpandingAmbiguitiesInto("TTT", "TYT");
    // W
    checkExpandingAmbiguitiesInto("AAA", "AWA");
    checkExpandingAmbiguitiesInto("TTT", "TWT");
    // S
    checkExpandingAmbiguitiesInto("CCC", "CSC");
    checkExpandingAmbiguitiesInto("GGG", "GSG");
    // K
    checkExpandingAmbiguitiesInto("GGG", "GKG");
    checkExpandingAmbiguitiesInto("TTT", "TKT");
    // M
    checkExpandingAmbiguitiesInto("AAA", "AMA");
    checkExpandingAmbiguitiesInto("CCC", "CMC");
    // D
    checkExpandingAmbiguitiesInto("AAA", "ADA");
    checkExpandingAmbiguitiesInto("GGG", "GDG");
    checkExpandingAmbiguitiesInto("TTT", "TDT");
    // V
    checkExpandingAmbiguitiesInto("AAA", "AVA");
    checkExpandingAmbiguitiesInto("CCC", "CVC");
    checkExpandingAmbiguitiesInto("GGG", "GVG");
    // H
    checkExpandingAmbiguitiesInto("AAA", "AHA");
    checkExpandingAmbiguitiesInto("CCC", "CHC");
    checkExpandingAmbiguitiesInto("TTT", "THT");
    // B
    checkExpandingAmbiguitiesInto("CCC", "CBC");
    checkExpandingAmbiguitiesInto("GGG", "GBG");
    checkExpandingAmbiguitiesInto("TTT", "TBT");

  }


  // This function tests that if ambiguities are only partially ambiguous, we are willing to expand many nearby ambiguities
  @Test
  public void checkManyPartialAmbiguities() {
    checkExpandingAmbiguitiesInto("AAAAAA", "ARRRRA");
  }

  // adds ambiguities to the sequence and checks that HashBlock_Stream still emits a matching hashblock
  private void checkExpandingAmbiguities(String text, int maxNumAmbiguities) {
    List<HashBlock> options = hashString(text, null);
    if (options.size() != 1) {
      // We don't have a hashblock that spans the entire sequence
      return;
    }
    List<String> sequenceWithAmbiguities = addNsUpTo(text, maxNumAmbiguities);
    for (String ambiguous : sequenceWithAmbiguities) {
      checkExpandingAmbiguitiesInto(text, ambiguous);
    }
  }

  private void checkExpandingAmbiguitiesInto(String text, String ambiguous) {
    List<HashBlock> options = hashString(text, null);
    if (options.size() != 1) {
      // We don't have a hashblock that spans the entire sequence
      Assert.fail("Not a hashblock: '" + text + "'");
    }
    HashBlock hashBlock = options.get(0);
    System.err.println("Checking '" + ambiguous + "'");
    List<HashBlock> ambiguousHashed = hashString(ambiguous, null);
    if (!blockContains(ambiguousHashed, hashBlock)) {
      StringBuilder message = new StringBuilder();
      message.append("Did not expand ambiguous text '" + ambiguous + "' into '" + text + "'. Expected hash code " + hashBlock.getForwardHash() + ". ");
      message.append("Expanded into these " + ambiguousHashed.size() + " possibilities:");
      for (HashBlock possibility : ambiguousHashed) {
        message.append("\n");
        message.append(possibility.getStartIndex() + ":" + possibility.getEndIndex() + ", hash = " + possibility.getForwardHash());
      }
      StringBuilder logger1 = new StringBuilder();
      hashString(text, logger1);
      message.append("\nHashing original text: " + logger1.toString());
      StringBuilder logger2 = new StringBuilder();
      hashString(ambiguous, logger2);
      message.append("\nHashing ambiguous text: " + logger2.toString());
      Assert.fail(message.toString());
    }
  }


  private boolean blockContains(List<HashBlock> container, HashBlock item) {
    for (HashBlock possibility : container) {
      if (possibility.getStartIndex() == item.getStartIndex() && possibility.getEndIndex() == item.getEndIndex() && possibility.getForwardHash() == item.getForwardHash()) {
        return true;
      }
    }
    return false;

  }

  private List<HashBlock> hashString(String text, StringBuilder logger) {
    Sequence sequence = new SequenceBuilder().setName("q").add(text).build();
    HashBlock_Stream stream = new HashBlock_Stream(sequence, true, null);
    List<HashBlock> results = new ArrayList<HashBlock>();
    int rowIndex = 0;
    while (true) {
      HashBlock_Row row = stream.getNextBatch();
      if (row == null)
        break;
      IMultiHashBlock block = row.get(0);
      if (block == null)
        break;
      for (ConditionalHashBlock conditional : block.getPossibilities()) {
        HashBlock possibility = conditional.getHashBlock();
        if (possibility != null) {
          if (possibility.getEndIndex() == sequence.getLength()) {
            results.add(possibility);
          }
        }
      }
      if (logger != null) {
        logger.append("\n");
        logger.append("" + rowIndex + ":");
        boolean first = true;
        for (int i = 0; i < text.length(); i++) {
          IMultiHashBlock blockI = row.get(i);
          if (blockI != null) {
            if (first) {
              first = false;
            } else {
              logger.append("; ");
            }
            logger.append(blockI.toString(sequence));
          }
        }
      }
      rowIndex++;
    }
    return results;
  }


  // generates all ways to add up to <maxNumAmbiguities> to <text> and returns them
  private List<String> addNsUpTo(String text, int maxNumAmbiguities) {
    List<String> results = new ArrayList<String>();
    for (int i = 0; i <= maxNumAmbiguities; i++) {
      results.addAll(addAmbiguities("", text, i));
    }
    return results;
  }

  // generates all ways to modify <text> by adding <numAmbiguities> into it, and adds <prefix> to each and returns the result
  private List<String> addAmbiguities(String prefix, String text, int numAmbiguities) {
    List<String> results = new ArrayList<String>();
    if (numAmbiguities < 1) {
      results.add(prefix + text);
      return results;
    }
    if (numAmbiguities > text.length()) {
      return results;
    }

    results.addAll(addAmbiguities(prefix + "N", text.substring(1), numAmbiguities - 1));
    results.addAll(addAmbiguities(prefix + text.substring(0, 1), text.substring(1), numAmbiguities));
    return results;
  }


}
