package mapper;

import java.util.ArrayList;
import java.util.List;

public class QueryMatch {
  public QueryMatch(SequenceMatch component, int priority) {
    this.components = new ArrayList<SequenceMatch>(1);
    this.components.add(component);
    this.priority = priority;
  }

  public QueryMatch(List<SequenceMatch> components, int priority, boolean hintCheckComponentsInForwardOrder) {
    this.components = components;
    this.priority = priority;
    this.hintCheckComponentsInForwardOrder = hintCheckComponentsInForwardOrder;
  }

  public QueryMatch(SequenceMatch component1, SequenceMatch component2, int priority, boolean hintCheckComponentsInForwardOrder) {
    this.components = new ArrayList<SequenceMatch>(2);
    this.components.add(component1);
    this.components.add(component2);
    this.priority = priority;
    this.hintCheckComponentsInForwardOrder = hintCheckComponentsInForwardOrder;
  }

  public List<SequenceMatch> getComponents() {
    return this.components;
  }
  public SequenceMatch getComponent(int index) {
    return this.components.get(index);
  }
  public int getNumSequences() {
    return this.components.size();
  }
  public int getPriority() {
    return this.priority;
  }

  public int getQueryTotalLength() {
    int total = 0;
    for (SequenceMatch match: this.components) {
      total += match.getSequenceA().getLength();
    }
    return total;
  }

  public int getStartIndexB() {
    SequenceMatch last = this.components.get(this.components.size() - 1);
    SequenceMatch first = this.components.get(0);
    return Math.min(first.getStartIndexB(), last.getStartIndexB());
  }

  public int getEndIndexB() {
    SequenceMatch last = this.components.get(this.components.size() - 1);
    SequenceMatch first = this.components.get(0);
    return Math.max(first.getStartIndexB(), last.getStartIndexB());
  }

  public int getTotalDistanceAcross() {
    SequenceMatch last = this.components.get(this.components.size() - 1);
    SequenceMatch first = this.components.get(0);
    if (this.getReversed())
      return first.getEndIndexB() - last.getStartIndexB();
    else
      return last.getEndIndexB() - first.getStartIndexB();
  }

  // Returns the total inner distance between subsequent pairs of components
  public int getTotalDistanceBetweenComponents() {
    int totalDistance = 0;
    SequenceMatch previousComponent = this.components.get(0);
    for (int i = 1; i < this.components.size(); i++) {
      SequenceMatch currentComponent = this.components.get(i);
      totalDistance += getDistance(previousComponent, currentComponent);
      previousComponent = currentComponent;
    }
    return totalDistance;
  }

  public boolean samePosition(QueryMatch other) {
    if (this.reversed != other.reversed)
      return false;
    if (this.components.size() != other.components.size()) {
      return false;
    }
    for (int i = 0; i < components.size(); i++) {
      if (!components.get(i).equals(other.components.get(i))) {
        return false;
      }
    }
    return true;
  }

  public String summarizePositionB() {
    String result = null;
    for (SequenceMatch component: this.components) {
      String append = component.summarizePositionB();
      if (result == null)
        result = append;
      else
        result = result + " / " + append;
    }
    return result;
  }

  public boolean get_hintCheckComponentsInForwardOrder() {
    return hintCheckComponentsInForwardOrder;
  }

  // Returns the distance between the two blocks
  // Can return a negative number if they overlap
  private int getDistance(SequenceMatch a, SequenceMatch b) {
    if (a.getSequenceB() != b.getSequenceB())
      return Integer.MAX_VALUE;
    int difference;
    if (this.getReversed())
      difference = a.getStartIndexB() - b.getEndIndexB();
    else
      difference = b.getStartIndexB() - a.getEndIndexB();
    return difference;
  }

  private boolean getReversed() {
    return this.components.get(0).getReversed();
  }

  private List<SequenceMatch> components;
  private int priority;
  private boolean reversed;
  private boolean hintCheckComponentsInForwardOrder;
}
