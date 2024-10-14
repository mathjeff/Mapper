package mapper;

// a SimilarityAnalysis is used by AncestryDetector when looking for similar parts of a reference genome
public class SimilarityAnalysis {
  public SimilarityAnalysis(Sequence sequence, int startIndex, int boundIndex, double initialScore) {
    this.sequence = sequence;

    this.startIndex = startIndex;
    this.boundIndex = boundIndex;
    this.currentIndex = startIndex;
    this.bestIndex = startIndex;

    this.cumulativeScore = initialScore;
    this.bestScore = initialScore;
  }

  public void addScore(double scoreHere) {
    this.cumulativeScore += scoreHere;

    if (this.cumulativeScore > this.bestScore) {
      this.bestScore = this.cumulativeScore;
      this.bestIndex = this.currentIndex;
    }
  }

  public boolean getReachedEndOfSequence() {
    return this.currentIndex < 0 || this.currentIndex >= this.sequence.getLength();
  }

  public Sequence sequence;

  public int startIndex;
  public int boundIndex;

  public double cumulativeScore;
  public int currentIndex;

  public double bestScore;
  public int bestIndex;

  @Override
  public String toString() {
    return "SimilarityAnalysis on " + this.sequence.getName() + " from " + this.startIndex + " to " + this.boundIndex;
  }
}
