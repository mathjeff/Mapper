package mapper;

import java.io.BufferedReader;
import java.io.IOException;

// A FastqParser parses .fastq files
public class FastqParser implements SequenceProvider {
  public FastqParser(BufferedReader reader, String path, boolean keepQualityData) {
    this.reader = reader;
    this.path = path;
    this.keepQualityData = keepQualityData;
  }

  public SequenceBuilder getNextSequence() {
    try {
      return doGetNextSequence();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean get_allReadsContainQualityInformation() {
    return true;
  }

  private SequenceBuilder doGetNextSequence() throws IOException {
    int first = this.reader.read();
    if (first < 0) {
      // End of file: no more entries
      return null;
    }
    char firstChar = (char)first;
    if (firstChar != '@') {
      // This is not the start of a new sequence
      // Could this be a blank line at the end of the file?
      if (firstChar == '\n') {
        int second = this.reader.read();
        if (second < 0) {
          return null;
        }
        char secondChar = (char)second;
        throw new IllegalArgumentException("Not in .fastq format: " + path + ". After newline expected end of file, not '" + secondChar + "'");
      }
      throw new IllegalArgumentException("Not in .fastq format: " + path + ". Expected '@', not '" + firstChar + "'");
    }
    String name = this.reader.readLine();
    String nameSuffix;
    int spaceIndex = name.indexOf(' ');
    if (spaceIndex != -1) {
      nameSuffix = name.substring(spaceIndex);
      name = name.substring(0, spaceIndex);
    } else {
      nameSuffix = "";
    }

    // read the sequence text and compress it
    String contentLine = this.reader.readLine();
    SequenceBuilder builder = new SequenceBuilder();
    builder.setName(name);
    builder.add(contentLine);

    // read the other lines
    String commentString = this.reader.readLine();
    String qualityString = this.reader.readLine();
    // build result and return it
    if (this.keepQualityData) {
      builder.asRead(nameSuffix, qualityString, commentString);
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
  boolean keepQualityData;
}
