package mapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.lang.Iterable;
import java.util.List;

class Duplication implements Comparable<Duplication> {
  public Duplication(int length) {
    this.length = length;
    // disallow duplicate positions, which can happen occasionally if two gapped hashblocks have exactly the same start and end position
    this.startPositions = new ArrayList<SequencePosition>();
  }

  public void addPosition(SequencePosition startPosition) {
    this.startPositions.add(startPosition);
  }

  public List<SequencePosition> getStartPositions() {
    return this.startPositions;
  }

  public void removeDuplicatePositions() {
    this.startPositions = new ArrayList<SequencePosition>(new HashSet<SequencePosition>(this.startPositions));
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

  @Override
  public int compareTo(Duplication other) {
    if (this.length != other.length) {
      return this.length - other.length;
    }
    if (this.startPositions.size() != other.startPositions.size()) {
      return this.startPositions.size() - other.startPositions.size();
    }
    for (int i = 0; i < this.startPositions.size(); i++) {
      int comparison = this.startPositions.get(i).compareTo(other.startPositions.get(i));
      if (comparison != 0)
        return comparison;
    }
    return 0;
  }

  private int length;
  private List<SequencePosition> startPositions;
}
