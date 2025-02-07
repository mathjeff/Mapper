package mapper;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

// A Deserializer reads information from a file
// It doesn't worry about supporting reading information written by a previous version of the code, because we don't need that
public class Deserializer {
  public Deserializer(File file) throws IOException {
    this.inputStream = new BufferedInputStream(new GZIPInputStream(new FileInputStream(file)));
  }

  public String readProperty(String name) throws IOException, InvalidObjectException {
    readText(name);
    readText(":");
    return readUntil(',');
  }

  public int readIntProperty(String name) throws IOException, InvalidObjectException {
    return Integer.parseInt(readProperty(name));
  }

  public byte[] readLengthPrefixedByteArray() throws IOException, InvalidObjectException {
    String lengthText = readUntil(':');
    int length = Integer.parseInt(lengthText);
    if (length < 0) {
      throw new InvalidObjectException("length = " + length + " < 0");
    }
    return readBytes(length);
  }

  public void readText(String expectedText) throws IOException, InvalidObjectException {
    byte[] expectedBytes = expectedText.getBytes();
    byte[] actualBytes = readBytes(expectedBytes.length);
    if (!Arrays.equals(expectedBytes, actualBytes)) {
      String actualText = new String(actualBytes);
      throw new InvalidObjectException("Expected '" + expectedText + "' but got '" + actualText + "'");
    }
  }

  // Reads characters until the given delimiter is encountered
  // Returns a string of the characters encountered before that delimiter
  public String readUntil(char delimiter) throws IOException {
    StringBuilder stringBuilder = new StringBuilder();
    while (true) {
      int current = this.inputStream.read();
      if (current == delimiter)
        return stringBuilder.toString();
      stringBuilder.append((char)current);
    }
  }

  public byte[] readBytes(int length) throws IOException {
    byte[] result = new byte[length];
    this.inputStream.read(result, 0, length);
    return result;
  }

  public void close() throws IOException {
    this.inputStream.close();
  }

  private BufferedInputStream inputStream;
}
