package mapper;

public class Distribution {
  public Distribution() {
  }

  public double getMean() {
    if (this.sumWeight != 0)
      return this.sumValue / this.sumWeight;
    return 0;
  }

  public double getVariance() {
    if (this.sumWeight == 0)
      return 0; // no data
    double variance = (this.sumSquaredValue - this.sumValue * this.sumValue / this.sumWeight) / sumWeight;
    if (variance < 0)
      return 0; // rounding error
    return variance;
  }

  public double getStdDev() {
    return Math.sqrt(this.getVariance());
  }

  public double getWeight() {
    return this.sumWeight;
  }

  public Distribution plus(Distribution other) {
    Distribution sum = new Distribution();
    sum.sumValue = this.sumValue + other.sumValue;
    sum.sumSquaredValue = this.sumSquaredValue + other.sumSquaredValue;
    sum.sumWeight = this.sumWeight + other.sumWeight;
    return sum;
  }

  public void add(double newValue) {
    this.add(newValue, 1);
  }

  public void add(double newValue, double weight) {
    this.sumValue += (newValue * weight);
    this.sumSquaredValue += (newValue * newValue * weight);
    this.sumWeight += weight;
  }

  private double sumValue;
  private double sumSquaredValue;
  private double sumWeight;
}
