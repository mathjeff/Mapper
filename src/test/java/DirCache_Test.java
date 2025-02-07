package mapper;

import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class DirCache_Test {
  @Test
  public void consistencyTest() throws IOException {
    DirCache cache = new DirCache(new File("/tmp/cache"), new MemoryFilesystem());
    Map<String, TreeMap<String, String>> cachePaths = new HashMap<String, TreeMap<String, String>>();
    int numEntries = 1000;

    // save a bunch of cache entries and make sure they're all different
    for (int i = 0; i < numEntries; i++) {
      TreeMap<String, String> properties = makeProperties(i);
      File dir = cache.getOrCreateDir(properties);
      String path = dir.getAbsolutePath();
      TreeMap<String, String> previousProperties = cachePaths.get(path);
      if (previousProperties != null)
        fail("Properties " + properties + " and " + previousProperties + " were both saved at " + path);
      cachePaths.put(path, properties);
    }
    // check the same cache entries and make sure they all already exist
    for (int i = 0; i < numEntries; i++) {
      TreeMap<String, String> properties = makeProperties(i);
      File dir = cache.getOrCreateDir(properties);
      String path = dir.getAbsolutePath();
      if (!cachePaths.containsKey(path))
        fail("Searched at " + path + " for properties " + properties + " but nothing was previously saved there");
      TreeMap<String, String> previousProperties = cachePaths.get(path);
      if (!properties.equals(previousProperties))
        fail("Searched at " + path + " for properties " + properties + ", but previously the properties saved there were different: " + previousProperties);
    }
  }

  private TreeMap<String, String> makeProperties(int i) {
    TreeMap<String, String> map = new TreeMap<String, String>();
    map.put("zeros", "" + (i % 10));
    map.put("tens", "" + ((i / 10) % 10));
    map.put("hundreds", "" + ((i / 100) % 10));
    return map;
  }

  private void fail(String message) {
    Assert.fail(message);
  }
}
