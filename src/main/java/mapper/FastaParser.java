package mapper;

import java.io.BufferedReader;
import java.io.IOException;

// A FastaParser parses .fasta files
public class FastaParser implements SequenceProvider {
  public FastaParser(BufferedReader reader, String path) {
    this.reader = reader;
    this.path = path;
  }

  public SequenceBuilder getNextSequence() {
    try {
      return doGetNextSequence();
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean get_allReadsContainQualityInformation() {
    return false;
  }

  private SequenceBuilder doGetNextSequence() throws IOException {
    // find sequence start marker
    if (!hasReadASequence) {
      int first = reader.read();
      if (first < 0)
        return null;
      char firstChar = (char)first;
      if (firstChar != '>') {
        throw new IllegalArgumentException("Not in .fasta format: expected first character to be '>', not '" + firstChar + "'");
      }
      hasReadASequence = true;
    }

    // read sequence name
    String name = reader.readLine();
    if (name == null)
      return null;
    int spaceIndex = name.indexOf(' ');
    if (spaceIndex > 0)
      name = name.substring(0, spaceIndex);
    SequenceBuilder builder = new SequenceBuilder();
    builder.setName(name);
    builder.setPath(path);

    // read sequence content until next sequence start marker
    while (true) {
      int firstInLine = reader.read();
      if (firstInLine < 0) {
        break;
      }
      char firstCharInLine = (char)firstInLine;
      if (firstCharInLine == '>') {
        break;
      }
      if (firstCharInLine == '\n') {
        // blank line: skip
        continue;
      }
      builder.add(firstCharInLine);
      String line = this.reader.readLine();
      if (line == null) {
        // remainder of line is empty: skip
        continue;
      }
      builder.add(line);
    }
    if (builder.getLength() < 1) {
      throw new RuntimeException("Sequence " + name + " in " + this.path + " has length " + builder.getLength());
    }
    return builder;
  }

  @Override
  public String toString() {
    return this.path;
  }

  BufferedReader reader;
  String path;
  boolean hasReadASequence = false;
}
