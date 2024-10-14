package mapper;

// A ReadSequence is a Sequence that has been read from somewhere
// A ReadSequence can contain quality information and comments
// A ReadSequence represents an entry in a .fastq file
public class ReadSequence extends Sequence {
  public ReadSequence(String name, String packedContents, int length, String path) {
    super(name, packedContents, length, path);
  }

  public String nameSuffix;
  public String qualityString;
  public String commentString;
}
