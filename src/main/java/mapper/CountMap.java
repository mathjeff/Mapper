package mapper;

import java.util.Map;
import java.util.HashMap;

// counts the most popular item
// Usually faster than HashMap<Integer, Integer>
public class CountMap {
  public CountMap() {
  }

  public void add(int key, int value) {
    if (key == mostPopularKey || mostPopularKey_count == 0) {
      mostPopularKey_count += value;
      mostPopularKey = key;
      if (counts != null)
        counts.put(mostPopularKey, mostPopularKey_count);
    } else {
      if (counts == null) {
        counts = new HashMap<Integer, Integer>();
        counts.put(mostPopularKey, mostPopularKey_count);
      }
      Integer count = counts.get(key);
      if (count == null) {
        count = value;
      } else {
        count += value;
      }
      counts.put(key, count);
      if (count > mostPopularKey_count) {
        mostPopularKey = key;
        mostPopularKey_count = count;
      }
    }
  }

  public int getMaxPopularity() {
    return mostPopularKey_count;
  }
  public int getMostPopularKey() {
    return mostPopularKey;
  }

  private int mostPopularKey;
  private int mostPopularKey_count;

  private Map<Integer, Integer> counts;
}
