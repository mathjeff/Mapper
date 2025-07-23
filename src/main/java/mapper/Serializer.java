package mapper;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

// A Serializer writes information to a file
// It doesn't worry about making sure the written data can still be read by a future version of the code, because we don't need that
public class Serializer {
  public Serializer(File file) throws IOException {
    this.outputFile = file;
    this.tempOutputFile = new File(file.toString() + ".tmp");
    this.outputStream = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(tempOutputFile)));
  }

  public void writeBytesAndLength(byte[] data) throws IOException {
    int length;
    if (data != null)
      length = data.length;
    else
      length = 0;
    this.writeString("" + length + ":");
    if (data != null)
      this.writeBytes(data);
  }
  public void writeBytes(byte[] data) throws IOException {
    this.outputStream.write(data);
  }

  public void writeString(String text) throws IOException {
    this.writeBytes(text.getBytes());
  }

  public void writeProperty(String key, String value) throws IOException {
    this.writeString(key);
    this.writeString(":");
    this.writeString(value);
    this.writeString(",");
  }

  public void close() throws IOException {
    this.outputStream.close();
    this.tempOutputFile.renameTo(this.outputFile);
  }

  private BufferedOutputStream outputStream;
  private File outputFile;
  private File tempOutputFile;
}
