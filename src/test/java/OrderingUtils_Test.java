package mapper;

import org.junit.Assert;
import org.junit.Test;

public class OrderingUtils_Test {
  @Test
  public void test1() {
    // make some positions
    int length = 20;
    Sequence sequence = new RepeatingSequence("test", 'A', length);
    SequencePosition[] positions1 = makePositions(sequence, length, 1);
    SequencePosition[] positions3 = makePositions(sequence, length, 3);
    SequencePosition[] positions7 = makePositions(sequence, length, 7);
    SequencePosition[] positions9 = makePositions(sequence, length, 9);
    SequencePosition[] positions11 = makePositions(sequence, length, 11);
    SequencePosition[] positions13 = makePositions(sequence, length, 13);
    SequencePosition[] positions17 = makePositions(sequence, length, 17);
    SequencePosition[] positions19 = makePositions(sequence, length, 19);
    test(positions1, positions3);
    test(positions1, positions7);
    test(positions1, positions9);
    test(positions1, positions11);
    test(positions1, positions13);
    test(positions1, positions17);
    test(positions1, positions19);
  }

  private SequencePosition[] makePositions(Sequence sequence, int length, int orderMultiplier) {
    SequencePosition[] positions = new SequencePosition[length];
    for (int i = 0; i < positions.length; i++) {
      int index = i * orderMultiplier % length;
      if (positions[index] != null) {
        throw new IllegalArgumentException("length " + length + " does not support orderMultiplier " + orderMultiplier + ": multiple positions attempting to go to index " + index + ". length and orderMultiplier shouldn't share any factors.");
      }
      int position = i * 6;
      positions[index] = new SequencePosition(sequence, position);
    }
    return positions;
  }

  private void test(SequencePosition[] a, SequencePosition[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException("a.length = " + a.length + ", b.length = " + b.length);
    }
    a = OrderingUtils.orderDeterministically(a);
    b = OrderingUtils.orderDeterministically(b);
    for (int i = 0; i < a.length; i++) {
      if (!(a[i].equals(b[i]))) {
        String error = "a[" + i + "] = " + a[i] + ", b[" + i + "] = " + b[i] + ". Full contents:\n";
        error += "a = ";
        for (int j = 0; j < a.length; j++) {
          error += a[j] + ",";
        }
        error += "\n";
        error += "b = ";
        for (int j = 0; j < b.length; j++) {
          error += b[j] + ",";
        }
        error += "\n";
        fail(error);
      }
    }
  }

  private void fail(String message) {
    Assert.fail(message);
  }
}
