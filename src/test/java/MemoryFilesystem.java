package mapper;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class MemoryFilesystem implements Filesystem {
  public boolean createNewFile(File file) {
    String key = getKey(file);
    if (this.fileContents.containsKey(key))
      return false;
    this.fileContents.put(key, new byte[0]);
    return true;
  }
  
  public void write(File file, byte[] contents) {
    String key = getKey(file);
    this.fileContents.put(key, contents);
  }

  public byte[] readFile(File file) {
    return this.fileContents.get(file.getAbsolutePath());
  }

  public void mkdirs(File dir) {
  }

  private String getKey(File file) {
    return file.getAbsolutePath();
  }

  private Map<String, byte[]> fileContents = new HashMap<String, byte[]>();
}
