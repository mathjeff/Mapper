package mapper;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class FastqWriter implements SequenceWriter {
  public FastqWriter(String path) throws FileNotFoundException {
    File file = new File(path);
    this.fileStream = new FileOutputStream(file);
    this.bufferedStream = new BufferedOutputStream(fileStream);
  }

  public void write(Sequence sequence) {
    ReadSequence read = (ReadSequence)sequence;
    this.write("@" + read.getName() + read.nameSuffix);
    this.write(read.getText());
    this.write(read.commentString);
    this.write(read.qualityString);
  }

  private void write(String text) {
    try {
      this.bufferedStream.write((text + "\n").getBytes());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    try {
      this.bufferedStream.close();
    } catch (IOException e) {
    }
    try {
      this.fileStream.close();
    } catch (IOException e) {
    }
  }

  FileOutputStream fileStream;
  BufferedOutputStream bufferedStream;
}
