package mapper;

public class HashBlock_CompilerCache {

  public static HashBlock_CompilerCache getInstance(int level) {
    HashBlock_CompilerCache cache = instances[level];
    if (cache == null) {
      synchronized(instances) {
        cache = instances[level];
        if (cache == null) {
          cache = new HashBlock_CompilerCache();
          instances[level] = cache;
        }
      }
    }
    return cache;
  }
  private static HashBlock_CompilerCache[] instances = new HashBlock_CompilerCache[100];

  public HashBlock_CompilerNode rootNode = new HashBlock_CompilerNode(null);
}
