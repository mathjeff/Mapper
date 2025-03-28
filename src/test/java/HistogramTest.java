package mapper;

import org.junit.Assert;
import org.junit.Test;


public class HistogramTest {
  public HistogramTest() {
  }

  @Test
  public void testSquash_unchanged() {
    checkSquash(new double[]{1, 2, 3, 4, 5}, new double[]{1, 2, 3, 4, 5});
  }

  @Test
  public void testSquash_squash6To3() {
    checkSquash(new double[]{4, 1, 6, 5, 3, 4}, new double[]{5, 11, 7});
  }

  @Test
  public void testSquash_squash3To2() {
    checkSquash(new double[]{1, 2, 4}, new double[]{2, 5});
  }

  @Test
  public void testSquash_squash4To3() {
    checkSquash(new double[]{3, 6, 9, 3}, new double[]{5, 10, 6});
  }

  @Test
  public void testSquash_squash0to1() {
    checkSquash(new double[]{}, new double[]{0});
  }

  @Test
  public void testSquash_stretch() {
    checkSquash(new double[]{4, 4, 4}, new double[]{3, 3, 3, 3});
  }

  private void checkSquash(double[] data, double[] expectedSquashed) {
    int desiredNumBins = expectedSquashed.length;
    double[] squashed = Histogram.squash(data, desiredNumBins);
    if (!equivalent(squashed, expectedSquashed)) {
      fail("Squashed " + toString(data) + " into " + desiredNumBins + " bins and got " + toString(squashed) + " instead of " + toString(expectedSquashed));
    }
  }

  private boolean equivalent(double[] a, double[] b) {
    if (a.length != b.length) {
      return false;
    }
    for (int i = 0; i < a.length; i++) {
      if (Math.abs(a[i] - b[i]) > 0.001) // ignore rounding error
        return false;
    }
    return true;
  }

  private String toString(double[] a) {
    StringBuilder result = new StringBuilder();
    result.append("[");
    for (int i = 0; i < a.length; i++) {
      if (i > 0)
        result.append(",");
      result.append("" + a[i]);
    }
    result.append("]");
    return result.toString();
  }

  private void fail(String message) {
    Assert.fail(message);
  }
}
