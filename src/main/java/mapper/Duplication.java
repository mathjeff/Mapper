package mapper;

import java.lang.Iterable;
import java.util.TreeSet;

class Duplication {
  public Duplication(int length) {
    this.length = length;
    // disallow duplicate positions, which can happen occasionally if two gapped hashblocks have exactly the same start and end position
    this.startPositions = new TreeSet<SequencePosition>();
  }

  public void addPosition(SequencePosition startPosition) {
    this.startPositions.add(startPosition);
  }

  public Iterable<SequencePosition> getStartPositions() {
    return this.startPositions;
  }

  public int getLength() {
    return length;
  }

  public int getNumInstances() {
    return this.startPositions.size();
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("Duplication length " + this.length + " at ");
    for (SequencePosition position : this.startPositions) {
      stringBuilder.append(position.toString());
      stringBuilder.append(",");
    }
    return stringBuilder.toString();
  }

  private int length;
  private TreeSet<SequencePosition> startPositions;
}
