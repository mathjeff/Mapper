package mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// An AlignmentsSection tells how various Sequences align to a particular section of a reference contig
public class AlignmentsSection {
  public AlignmentsSection(Sequence sequence) {
    this.sequence = sequence;
  }

  public void addForward(int referenceIndex, byte encodedValue, float weight, Sequence querySequence, int queryPosition, boolean nearQueryEnd) {
    if (nearQueryEnd)
      this.ensureEnd().addForward(referenceIndex, encodedValue, weight, querySequence, queryPosition);
    else
      this.ensureMiddle().addForward(referenceIndex, encodedValue, weight, querySequence, queryPosition);
  }
  public void addReverse(int referenceIndex, byte encodedValue, float weight, Sequence querySequence, int queryPosition, boolean nearQueryEnd) {
    if (nearQueryEnd)
      this.ensureEnd().addReverse(referenceIndex, encodedValue, weight, querySequence, queryPosition);
    else
      this.ensureMiddle().addReverse(referenceIndex, encodedValue, weight, querySequence, queryPosition);
  }
  public void insertForward(int referenceIndex, String value, float weight, Sequence querySequence, int queryPosition, boolean nearQueryEnd) {
    if (nearQueryEnd)
      this.ensureEnd().insertForward(referenceIndex, value, weight, querySequence, queryPosition);
    else
      this.ensureMiddle().insertForward(referenceIndex, value, weight, querySequence, queryPosition);
  }
  public void insertReverse(int referenceIndex, String value, float weight, Sequence querySequence, int queryPosition, boolean nearQueryEnd) {
    if (nearQueryEnd)
      this.ensureEnd().insertReverse(referenceIndex, value, weight, querySequence, queryPosition);
    else
      this.ensureMiddle().insertReverse(referenceIndex, value, weight, querySequence, queryPosition);
  }

  public AlignmentPosition getPosition(int referenceIndex) {
    AlignmentPosition position = new AlignmentPosition(this.sequence.charAt(referenceIndex));
    if (this.endAlignments != null)
      endAlignments.updateCount(position, referenceIndex, true);
    if (this.middleAlignments != null)
      middleAlignments.updateCount(position, referenceIndex, false);
    return position;
  }

  public AlignmentPosition getInsertion(int referenceIndex, int insertionIndex) {
    AlignmentPosition position = new AlignmentPosition('-');
    if (this.endAlignments != null)
      endAlignments.updateInsertionCount(position, referenceIndex, insertionIndex, true);
    if (middleAlignments != null)
      middleAlignments.updateInsertionCount(position, referenceIndex, insertionIndex, false);
    return position;
  }

  // returns a RegionAlignments that keeps track of alignments in the middle of queries
  private RegionAlignments ensureMiddle() {
    if (this.middleAlignments == null) {
      this.middleAlignments = new RegionAlignments(this.sequence);
    }
    return this.middleAlignments;
  }

  // returns a RegionAlignments that keeps track of alignments in the ends of queries
  private RegionAlignments ensureEnd() {
    if (this.endAlignments == null) {
      this.endAlignments = new RegionAlignments(this.sequence);
    }
    return this.endAlignments;
  }

  private Sequence sequence;
  private RegionAlignments middleAlignments;
  private RegionAlignments endAlignments;

  private static char[] allBases = new char[]{'A', 'C', 'G', 'T'};
}
