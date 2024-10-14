package mapper;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class FastaWriter implements SequenceWriter {
  public FastaWriter(String path) throws FileNotFoundException {
    File file = new File(path);
    this.fileStream = new FileOutputStream(file);
    this.bufferedStream = new BufferedOutputStream(fileStream);
  }

  public void write(Sequence sequence) {
    this.write(">" + sequence.getName());
    this.write(sequence.getText());
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

  private void write(String text) {
    try {
      this.bufferedStream.write((text + "\n").getBytes());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  FileOutputStream fileStream;
  BufferedOutputStream bufferedStream;
}
