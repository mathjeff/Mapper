package mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// A RegionAlignments tells how parts of Sequences align
// One RegionAlignments handles the middles of the queries and one RegionAlignments handles the ends of the queries
public class RegionAlignments {
  public RegionAlignments(Sequence sequence) {
    this.forwardAlignments = new DirectionalAlignments(sequence);
    this.reverseAlignments = new DirectionalAlignments(sequence);
  }
  public void addForward(int referenceIndex, byte encodedValue, float weight, Sequence querySequence, int queryPosition) {
    this.forwardAlignments.add(referenceIndex, encodedValue, weight, querySequence, queryPosition);
  }
  public void addReverse(int referenceIndex, byte encodedValue, float weight, Sequence querySequence, int queryPosition) {
    this.reverseAlignments.add(referenceIndex, encodedValue, weight, querySequence, queryPosition);
  }
  public void insertForward(int referenceIndex, String value, float weight, Sequence querySequence, int queryPosition) {
    this.forwardAlignments.insert(referenceIndex, value, weight, querySequence, queryPosition);
  }
  public void insertReverse(int referenceIndex, String value, float weight, Sequence querySequence, int queryPosition) {
    this.reverseAlignments.insert(referenceIndex, value, weight, querySequence, queryPosition);
  }

  public void updateCount(AlignmentPosition position, int referenceIndex, boolean nearQueryEnd) {
    reverseAlignments.updateCount(position, referenceIndex, false, nearQueryEnd);
    forwardAlignments.updateCount(position, referenceIndex, true, nearQueryEnd);
  }

  public void updateInsertionCount(AlignmentPosition position, int referenceIndex, int insertionIndex, boolean nearQueryEnd) {
    forwardAlignments.updateInsertionCount(position, referenceIndex, insertionIndex, true, nearQueryEnd);
    reverseAlignments.updateInsertionCount(position, referenceIndex, insertionIndex, false, nearQueryEnd);
  }

  private DirectionalAlignments forwardAlignments;
  private DirectionalAlignments reverseAlignments;
}
