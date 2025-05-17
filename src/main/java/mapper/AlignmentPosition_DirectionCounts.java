package mapper;

import java.util.ArrayList;
import java.util.List;

// An AlignmentPosition_DirectionCounts tells how many reads aligned to a specific position in a specific direction
public class AlignmentPosition_DirectionCounts {
  private static int listScale = 100;

  public void putScaledReference(float weight) {
    this.referenceCount = (int)weight;
  }

  public int getScaledReference() {
    return this.referenceCount;
  }

  public void putScaledAlternate(char value, int scaledWeight) {
    if (this.counts == null) {
      if (scaledWeight == 0)
        return;
      this.counts = this.newList();
    }
    int index = indexForKey(value);
    this.counts[index] = scaledWeight;
  }

  public int getScaledAlternate(char value) {
    if (this.counts == null)
      return 0;
    int index = indexForKey(value);
    return getScaledAlternate(index);
  }

  public int getScaledAlternate(int index) {
    if (this.counts == null)
      return 0;
    return this.counts[index];
  }

  public void ignoreAlternate(char value) {
    int alternateCount = this.getScaledAlternate(value);
    putScaledAlternate(value, 0);
    this.ignoredAlternateCount += alternateCount;
  }

  public boolean hasAlternates() {
    if (this.counts == null)
      return false;
    for (int i = 0; i < this.counts.length; i++) {
      if (this.counts[i] != 0)
        return true;
    }
    return false;
  }

  public float getAlternateCount(int index) {
    return ((float)getScaledAlternate(index)) / listScale;
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

  public float getIgnoredAlternateCount() {
    return (float)(this.ignoredAlternateCount) / listScale;
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
  // The number of items we have that don't match the reference but we've been asked to ignore
  private int ignoredAlternateCount;

  // Lists of items that don't match the reference
  // List indices match this.makeKeys()
  //private List<Integer> counts;
  private Integer[] counts;
}
