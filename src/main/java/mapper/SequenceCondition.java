package mapper;

import java.util.TreeMap;

// A SequenceCondition refers to the concept of a Sequence having a certain base pair at a certain position
// A SequenceCondition can evaluate to True or False
// A SequenceCondition might be useful in the case of ambiguous sequences (containing 'N')
public class SequenceCondition {
  public static SequenceCondition ALWAYS = new SequenceCondition();

  public SequenceCondition() {
    this.keys = new int[0];
    this.values = new char[0];
  }
  public SequenceCondition(int position, char value) {
    this.keys = new int[1];
    this.keys[0] = position;
    this.values = new char[1];
    this.values[0] = value;
  }
  private SequenceCondition(int[] keys, char[] values) {
    this.keys = keys;
    this.values = values;
  }

  // returns a SequenceCondition that returns <true> if and only if <this> and <other> return <true>
  public SequenceCondition intersect(SequenceCondition other) {
    // Check some simple cases
    if (other.values.length < 1)
      return this;
    if (this.values.length < 1)
      return other;
    if (this == other)
      return this;

    // check for conflicts
    int i = 0;
    int j = 0;
    int numMatchingKeys = 0;
    while (i < this.keys.length && j < other.keys.length) {
      int ourKey = this.keys[i];
      int theirKey = other.keys[j];
      if (ourKey < theirKey) {
        i++;
      } else {
        if (theirKey < ourKey) {
          j++;
        } else {
          char ourValue = this.values[i];
          char theirValue = other.values[j];
          if (ourValue != theirValue) {
            return null; // conflict
          }
          numMatchingKeys++;
          i++;
          j++;
        }
      }
    }
    // now compute the merge
    if (numMatchingKeys == this.keys.length)
      return other;
    if (numMatchingKeys == other.keys.length)
      return this;
    int mergedCapacity = this.keys.length + other.keys.length - numMatchingKeys;
    int[] mergedKeys = new int[mergedCapacity];
    char[] mergedValues = new char[mergedCapacity];

    int writeIndex = 0;
    i = j = 0;
    while (i < this.keys.length && j < other.keys.length) {
      int ourKey = this.keys[i];
      int theirKey = other.keys[j];
      if (ourKey < theirKey) {
        mergedKeys[writeIndex] = this.keys[i];
        mergedValues[writeIndex] = this.values[i];
        i++;
      } else {
        if (theirKey < ourKey) {
          mergedKeys[writeIndex] = other.keys[j];
          mergedValues[writeIndex] = other.values[j];
          j++;
        } else {
          mergedKeys[writeIndex] = this.keys[i];
          mergedValues[writeIndex] = this.values[i];
	  i++;
          j++;
        }
      }
      writeIndex++;
    }
    while (i < this.keys.length) {
      mergedKeys[writeIndex] = this.keys[i];
      mergedValues[writeIndex] = this.values[i];
      i++;
      writeIndex++;
    }
    while (j < other.keys.length) {
      mergedKeys[writeIndex] = other.keys[j];
      mergedValues[writeIndex] = other.values[j];
      j++;
      writeIndex++;
    }

    return new SequenceCondition(mergedKeys, mergedValues);
  }

  // Returns the number of positions that each must have a specific value in order to satisfy this condition
  // For, example, if this condition says "position 0 is 'A' and position 5 is 'C' " then complexity = 2
  public int getComplexity() {
    return this.values.length;
  }

  public SequenceCondition shifted(int shift) {
    SequenceCondition copy = new SequenceCondition();
    copy.values = this.values;
    copy.keys = new int[this.keys.length];
    for (int i = 0; i < this.keys.length; i++) {
      copy.keys[i] = this.keys[i] + shift;
    }
    return copy;
  }

  public String toString() {
    String result = "";
    for (int i = 0; i < this.keys.length; i++) {
      if (result.length() > 0)
        result += ",";
      result += "seq[" + this.keys[i] + "]=" + this.values[i];
    }
    return result;
  }

  private int[] keys;
  private char[] values;
}
