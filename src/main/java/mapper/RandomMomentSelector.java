package mapper;

import java.util.Random;

// Selects a random moment in time among all of the times that it is called
public class RandomMomentSelector {
  public RandomMomentSelector() {
    this.startTime = System.currentTimeMillis();
    this.random = new Random();
  }

  // Tells whether this moment has been selected.
  // If this function returns true, then this moment has been selected and any previous moments have been unselected
  public boolean select(long currentTimeMillis) {
    long elapsed = currentTimeMillis - this.startTime;
    if (elapsed >= this.targetDuration) {
      double divisor = random.nextFloat();
      if (divisor <= 0 || divisor >= 1)
        divisor = 0.5;
      this.targetDuration = elapsed / divisor;
      return true;
    }
    return false;
  }

  
  private Random random;
  private double targetDuration;
  private long startTime;
}
