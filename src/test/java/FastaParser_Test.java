package mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class FastaParser_Test {
  @Test
  public void testRemovesSpacesInContigName() {
    String text = ">sequence details\nACGT";

    List<Sequence> sequences = parse(text);
    if (sequences.size() != 1) {
      fail("Expected 1 sequence, not " + sequences.size());
    }
    Sequence sequence = sequences.get(0);
    String expectedName = "sequence";
    if (!sequence.getName().equals(expectedName)) {
      fail("Expected sequence to have name '" + expectedName + "', not '" + sequence.getName() + "'");
    }
  }

  private List<Sequence> parse(String text) {
    FastaParser reader = buildReader(text);
    List<Sequence> sequences = new ArrayList<Sequence>();
    while (true) {
      SequenceBuilder builder = reader.getNextSequence();
      if (builder == null)
        break;
      sequences.add(builder.build());
    }
    return sequences;
  }

  private FastaParser buildReader(String text) {
    StringReader stringReader = new StringReader(text);
    BufferedReader bufferedReader = new BufferedReader(stringReader);
    String path = "test";
    return new FastaParser(bufferedReader, path);
  }


  private void fail(String message) {
    Assert.fail(message);
  }
}
