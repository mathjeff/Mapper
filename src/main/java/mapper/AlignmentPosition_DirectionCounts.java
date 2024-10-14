package mapper;

import java.util.ArrayList;
import java.util.List;

// An AlignmentPosition_DirectionCounts tells how many reads aligned to a specific position in a specific direction
public class AlignmentPosition_DirectionCounts {
  private static int listScale = 100;

  public void putScaledReference(float weight) {
    this.referenceCount += weight;
  }

  public void putScaledAlternate(char value, int scaledWeight) {
    if (scaledWeight == 0)
      return;
    if (this.counts == null) {
      this.counts = this.newList();
    }
    int index = indexForKey(value);
    this.counts[index] = scaledWeight;
  }

  public boolean hasAlternates() {
    return this.counts != null;
  }

  public float getAlternateCount(int index) {
    return this.listGet(this.counts, index);
  }
  public float getAlternateCount(char value) {
    int index = this.indexForKey(value);
    return this.getAlternateCount(index);
  }

  public boolean hasAlternate(int index) {
    return this.getAlternateCount(index) > 0;
  }

  public float getReferenceCount() {
    return (float)(this.referenceCount) / listScale;
  }

  public static char[] getAllKeys() {
    if (allKeys == null) {
      allKeys = makeKeys();
    }
    return allKeys;
  }
  public static int getNumKeys() {
    return getAllKeys().length;
  }

  private static char[] allKeys;
  private static char[] makeKeys() {
    char[] result = new char[]{'A', 'C', 'G', 'T', 'N','-'};
    return result;
  }
  public static int indexForKey(char key) {
    char[] keys = getAllKeys();
    for (int i = 0; i < keys.length; i++) {
      if (key == keys[i]) {
        return i;
      }
    }
    return -1;
  }
  public static char keyForIndex(int index) {
    return getAllKeys()[index];
  }

  // treats null lists as empty and then does an index lookup and scales the value
  private float listGet(Integer[] list, int index) {
    if (list == null) {
      return 0;
    }
    return ((float)list[index]) / listScale;
  }
  private void listSet(Integer[] list, int index, float value) {
    list[index] = (int)(value * listScale);
  }
  private void listAdd(Integer[] list, int index, float value) {
    listSet(list, index, listGet(list, index) + value);
  }
  private Integer[] newList() {
    int numAltKeys = getAllKeys().length;
    Integer[] list = new Integer[numAltKeys];
    for (int i = 0; i < numAltKeys; i++) {
      list[i] = 0;
    }
    return list;
  }

  // The number of items we have that match the reference
  private int referenceCount;

  // Lists of items that don't match the reference
  // List indices match this.makeKeys()
  //private List<Integer> counts;
  private Integer[] counts;
}
