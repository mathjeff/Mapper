package mapper;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

class AlignmentCache {
  public AlignmentCache() {
  }

  public QueryAlignments get(Query query) {
    QueryAlignments result = this.cache.get(query);
    return result;
  }

  public void addAlignment(Query query, QueryAlignments alignments) {
    this.cache.put(query, alignments);
  }

  public int getUsage() {
    return this.cache.size();
  }

  public void addHits(int numHits) {
    synchronized(this.statsLock) {
      this.numHits += numHits;
    }
  }

  public long getNumHits() {
    return this.numHits;
  }

  private ConcurrentHashMap<Query, QueryAlignments> cache = new ConcurrentHashMap<Query, QueryAlignments>();
  private Object statsLock = new Object();
  private long numHits;
}
