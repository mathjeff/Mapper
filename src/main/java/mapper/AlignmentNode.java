package mapper;

// An AlignmentNode is used by a PathAligner when aligning two sequences
// An AlignmentNode refers to the current best known penalty of a position in the alignment path
public class AlignmentNode implements Comparable {
  public AlignmentNode(int x, int y, double penalty, double insertXPenalty, double insertYPenalty, boolean reachedMainDiagonal, boolean reachedOtherDiagonal) {
    this.x = x;
    this.y = y;
    this.penalty = penalty;
    this.insertXPenalty = insertXPenalty;
    this.insertYPenalty = insertYPenalty;
    this.reachedMainDiagonal = reachedMainDiagonal;
    this.reachedOtherDiagonal = reachedOtherDiagonal;
  }

  public int getX() {
    return x;
  }
  public int getY() {
    return y;
  }
  public double getPenalty() {
    return this.penalty;
  }
  public double getInsertXPenalty() {
    return this.insertXPenalty;
  }
  public double getInsertYPenalty() {
    return this.insertYPenalty;
  }
  public boolean getReachedMainDiagonal() {
    return this.reachedMainDiagonal;
  }
  public boolean getReachedOtherDiagonal() {
    return this.reachedOtherDiagonal;
  }

  public int compareTo(Object other) {
    AlignmentNode converted = (AlignmentNode)other;
    int a = Double.compare(this.penalty, converted.penalty);
    if (a != 0) {
      return a;
    }
    return Integer.compare(converted.x, this.x);
  }

  int x;
  int y;
  double penalty;
  double insertXPenalty;
  double insertYPenalty;
  boolean reachedMainDiagonal;
  boolean reachedOtherDiagonal;
}
