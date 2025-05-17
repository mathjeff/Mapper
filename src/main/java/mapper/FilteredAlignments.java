package mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// A FilteredAlignments tells how some subset of Sequences align to a particular reference contig
public class FilteredAlignments {
  public FilteredAlignments(Alignments alignments, MutationDetectionParameters filter) {
    this.alignments = alignments;
    this.filter = filter;
  }

  public int size() {
    return this.alignments.size();
  }

  public AlignmentPosition getPosition(int referenceIndex) {
    AlignmentPosition result = this.alignments.getPosition(referenceIndex);
    result = filterSNPs(result, referenceIndex);
    result = filterDeletions(result, referenceIndex);
    return result;
  }

  // TODO: make this more efficient rather than calling it separately for each position in the deletion
  private boolean couldBeDeletion(int referenceIndex) {
    int index = referenceIndex;
    while (index >= 0) {
      AlignmentPosition position = this.alignments.getPosition(index);
      if (this.filter.supportsIndelStart(position))
        return true;
      if (!this.filter.supportsIndelContinuation(position))
        return false;
      index--;
    }
    return true;
  }

  public AlignmentPosition getInsertion(int referenceIndex, int insertionIndex) {
    AlignmentPosition result = this.alignments.getInsertion(referenceIndex, insertionIndex);
    result = filterInsertions(result, referenceIndex, insertionIndex);
    return result;
  }

  private AlignmentPosition filterSNPs(AlignmentPosition position, int referenceIndex) {
    float totalCount = position.getCount();
    char[] nonzeroAlternates = position.getNonzeroAlternates();
    for (char alternate: nonzeroAlternates) {
      if (alternate != '-') {
        float alternateCount = position.getAlternateCount(alternate);
        if (!filter.supportsSNP(alternateCount, totalCount)) {
          position.ignoreAlternate(alternate);
        }
      }
    }
    return position;
  }

  private AlignmentPosition filterDeletions(AlignmentPosition position, int referenceIndex) {
    // fast path for positions without alternates
    if (!position.hasAlternates())
      return position;
    // check whether this position satisfies the filter
    if (couldBeDeletion(referenceIndex))
      return position;
    // filter out deletions
    position.ignoreAlternate('-');
    return position;
  }

  private AlignmentPosition filterInsertions(AlignmentPosition position, int referenceIndex, int insertionIndex) {
    // fast path for most positions without alternates
    if (!position.hasAlternates())
      return position;
    // check whether this position satisfies the filter
    boolean keepInsertions = false;
    if (insertionIndex == 0)
      keepInsertions = this.filter.supportsIndelStart(position);
    else
      keepInsertions = this.filter.supportsIndelContinuation(position);
    if (!keepInsertions) {
      char[] nonzeroAlternates = position.getNonzeroAlternates();
      for (char alternate: nonzeroAlternates) {
        position.ignoreAlternate(alternate);
      }
    }
    return position;
  }

  Alignments alignments;
  MutationDetectionParameters filter;
}
