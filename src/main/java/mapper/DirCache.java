package mapper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

// A DirCache allows doing a lookup by key and provides a unique directory to read and write values from
public class DirCache {
  public DirCache(File rootDir, Filesystem filesystem) {
    // append another directory name in case we want to further classify subdirectories in the future
    this.rootDir = new File(rootDir, "cache");
    this.filesystem = filesystem;
  }

  // Returns the directory for storing information about the given properties
  public File getOrCreateDir(TreeMap<String, String> properties) throws IOException {
    String propertiesText = propertiesToString(properties);
    byte[] propertiesBytes = stringToBytes(propertiesText);
    byte[] hashBytes = getHashes(propertiesBytes);
    String[] hashes = bytesToStrings(hashBytes);
    File currentDir = this.rootDir;

    // This shouldn't take long, so we avoid running it in parallel to make it simpler
    synchronized(this) {
      for (String hash: hashes) {
        if (tryWriteMetadata(propertiesBytes, currentDir)) {
          // We just wrote this metadata into this dir
          return getContentDir(currentDir);
        }
        if (metadataMatches(propertiesBytes, currentDir)) {
          // We previously wrote this metadata into this dir
          return getContentDir(currentDir);
        }
        currentDir = new File(currentDir, hash);
      }
    }
    throw new IllegalArgumentException("Cache error: could not identify unique dir starting from " + this.rootDir + " with properties text '" + propertiesText + "'. Reached " + currentDir + " (" + hashes.length + " hashes, " + propertiesBytes.length + " bytes)");
  }

  private String propertiesToString(TreeMap<String, String> properties) {
    // We require a TreeMap so the properties will be sorted
    StringBuilder builder = new StringBuilder();
    builder.append("{\n");
    // We don't need to support any special characters so we don't need to check for them
    for (Map.Entry<String, String> entry: properties.entrySet()) {
      builder.append(entry.getKey());
      builder.append(":\"");
      builder.append(entry.getValue());
      builder.append("\",\n");
    }
    builder.append("}");
    return builder.toString();
  }

  private byte[] stringToBytes(String string) {
    return string.getBytes();
  }

  // given an array of bytes, identifies a set of hashcodes such that:
  // 1. the full set of hashcodes should uniquely identify the byte array
  // 2. earlier hashcodes should be unlikely to produce hash collisions
  private byte[] getHashes(byte[] bytes) {
    byte[] current = bytes;
    byte[] result = new byte[bytes.length];
    int writeIndex = result.length - 1;
    // We build a pyramid of hashcodes starting at the bottom
    while (current.length > 0) {
      // Split bytes into even and odd
      byte[] even = new byte[(current.length + 1) / 2]; 
      byte[] odd = new byte[even.length];
      byte[] next = new byte[current.length / 2];
      for (int i = 0; i < current.length; i++) {
        if (i % 2 == 0)
          even[i / 2] = current[i];
        else
          odd[i / 2] = current[i];
      }
      // Emit left parents, and then xor parents to make new children
      for (int i = 0; i < even.length; i++) {
        result[writeIndex] = even[i];
        writeIndex--; // Write from the end to the beginning so that more interesting hashes are at the beginning
        if (i < next.length)
          next[i] = (byte)(even[i] ^ odd[i]);
      }
      current = next;
    }
    return result;
  }

  // converts an array of bytes into an array of strings
  private String[] bytesToStrings(byte[] bytes) {
    String[] strings = new String[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      strings[i] = byteToString(bytes[i]);
    }
    return strings;
  }

  // converts a byte into a short string
  private String byteToString(byte b) {
    int integer = b;
    if (integer < 0)
      integer += 256;
    int lastCharIndex = integer % 16;
    int firstCharIndex = (integer - lastCharIndex) / 16;
    String chars = "abcdefg0123456789";
    return "" + chars.charAt(firstCharIndex) + chars.charAt(lastCharIndex);
  }

  // Tries to write the given metadata to the metadata file for the given directory
  // Returns true if metadata was written successfully
  private boolean tryWriteMetadata(byte[] propertiesBytes, File dir) throws IOException {
    File metadataFile = new File(dir, keysFileName);
    if (!this.filesystem.createNewFile(metadataFile))
      return false;
    this.filesystem.write(metadataFile, propertiesBytes);
    return true;
  }

  // Tells whether the given property text matches the metadata file for the given directory
  private boolean metadataMatches(byte[] propertiesBytes, File dir) throws IOException {
    File metadataFile = new File(dir, keysFileName);
    byte[] existingBytes = this.filesystem.readFile(metadataFile);
    if (!Arrays.equals(propertiesBytes, existingBytes))
      return false;
    return true;
  }

  // Given a directory containing information relating to a cache entry, returns a directory to store the cache value
  private File getContentDir(File cacheDir) throws IOException {
    // We use a different directory for storing the cache values so:
    //   1. It's easier to inspect visually
    //   The cache entries are organized in a tree, and when we're looking for the right cache entries we're not necessarily interested in their values
    //   2. It means we don't have to worry about whether any hash of a cache entry conflicts with a file in the cache entry
    File contentDir = new File(cacheDir, contentFileName);
    this.filesystem.mkdirs(contentDir);
    this.recordUsage(cacheDir);
    return contentDir;
  }

  private void recordUsage(File cacheDir) throws IOException {
    File usageFile = new File(cacheDir, usageFilename);
    String text = "{read:\"" + System.currentTimeMillis() + "\"}";
    this.filesystem.write(usageFile, text.getBytes());
  }

  private File rootDir;
  private Filesystem filesystem;
  private static final String keysFileName = "keys";
  private static final String contentFileName = "content";
  private static final String usageFilename = "usage";
}
