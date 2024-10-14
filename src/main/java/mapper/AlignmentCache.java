package mapper;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

class AlignmentCache {
  public AlignmentCache() {
  }

  public QueryAlignments get(Query query) {
    QueryAlignments result = this.cache.get(query);
    /*if (result != null) {
      System.out.println("Got cache hit for query " + query.format());
      if (result.getNumQueries() == 1) {
        List<QueryAlignment> alignments = result.getFirstAlignments();
        if (alignments.size() == 1) {
          QueryAlignment alignment = alignments.get(0);
          System.out.println("Components:");
          for (SequenceAlignment sequenceAlignment: alignment.getComponents()) {
            System.out.println("Component:");
            System.out.println(sequenceAlignment.format());
          }
        }
      }
    }*/
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

  public long estimateNumUniqueQueries() {
    synchronized(this.statsLock) {
      long cacheSize = this.cache.size();
      // If there are N unique queries and we've tried sqrt(N) of them, we should get approximately 1 cache hit
      // If there are N unique queries and we've tried N of them, we should get approximately N cache hits
      return cacheSize * cacheSize / (this.numHits + 1);
    }
  }

  private ConcurrentHashMap<Query, QueryAlignments> cache = new ConcurrentHashMap<Query, QueryAlignments>();
  private Object statsLock = new Object();
  private long numHits;
}
