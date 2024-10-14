package mapper;

import org.junit.Assert;
import org.junit.Test;

public class HashBlockCompiler_Test {
  @Test
  public void shortTest() {
    check("AACGACGT");
  }

  @Test
  public void ambiguityTest() {
    // Ambiguity here:          .
    check("GGGGGAGCGACCAAACGGCAGSTTCACTCA");
  }

  // build a pyramid for this text, compile each row, and check that the compilation gives the same results
  private void check(String text) {
    Sequence sequence = new SequenceBuilder().setName("seq").add(text).build();
    HashBlock_Row row = new HashBlock_BaseRow(sequence, null);
    check(row);
  }

  // build a pyramid from this row, compile each resulting row, and check that the compilation gives the same results
  private void check(HashBlock_Row row) {
    while (true) {
      HashBlock_Row compiled = new HashBlock_Compiler(row, new HashBlock_CompilerCache());
      compare(row, compiled);
      compare(row, compiled);
      if (row.getAfter(-1) == null)
        break;
      row = new HashBlock_ParentRow(compiled, false, null);
    }
  }

  // checks that two rows are the same
  private void compare(HashBlock_Row rowA, HashBlock_Row rowB) {
    Sequence sequence = rowA.getSequence();
    for (int i = -1; i < sequence.getLength(); i++) {
      IMultiHashBlock blockA = rowA.getAfter(i);
      IMultiHashBlock blockB = rowB.getAfter(i);
      String stringA = null;
      if (blockA != null) {
        stringA = blockA.toString(sequence);
      }
      String stringB = null;
      if (blockB != null) {
        stringB = blockB.toString(sequence);
      }
      boolean equal = false;
      if ((stringA == null) != (stringB == null)) {
        equal = false;
      } else {
        if (stringA == null && stringB == null) {
          equal = true;
        } else {
          equal = stringA.equals(stringB);
        }
      }
      if (!equal) {
        fail("rowA.getAfter(" + i + ") = " + stringA + " but rowB.getAfter(" + i + ") = " + stringB);
      }
    }
  }

  private void fail(String message) {
    Assert.fail(message);
  }
}
