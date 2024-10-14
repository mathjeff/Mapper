package mapper;

import java.util.List;

public class AlignmentCounter implements AlignmentListener {
  public void addAlignments(List<List<QueryAlignment>> alignments) {
    int newNumMatchingSequences = 0;
    int newNumMatchingQueries = 0;
    double newTotalAlignedPenalty = 0;
    long newTotalAlignedQueryLength = 0;
    Distribution newTotalDistanceBetweenComponents = new Distribution();
    for (List<QueryAlignment> alignment : alignments) {
      if (alignment.size() > 0) {
        newNumMatchingQueries++;
        newNumMatchingSequences += alignment.get(0).getNumSequences();

        newTotalAlignedPenalty += alignment.get(0).getPenalty();
        newTotalAlignedQueryLength += alignment.get(0).getALength();

        double currentTotalDistanceBetweenComponents = 0;
        for (QueryAlignment choice: alignment) {
          newTotalDistanceBetweenComponents.add(choice.getTotalDistanceBetweenComponents(), (double)1.0 / (double)alignment.size());
        }
      }
    }
    synchronized (this) {
      this.numMatchingSequences += newNumMatchingSequences;
      this.numMatchingQueries += newNumMatchingQueries;
      this.totalAlignedPenalty += newTotalAlignedPenalty;
      this.totalAlignedQueryLength += newTotalAlignedQueryLength;
      this.distanceBetweenQueryComponents = this.distanceBetweenQueryComponents.plus(newTotalDistanceBetweenComponents);
    }
  }

  public void addUnaligned(List<Query> unalignedQueries) {
    int numNewUnalignedQueries = 0;
    for (Query query: unalignedQueries) {
      numNewUnalignedQueries += query.getNumSequences();
    }
    synchronized (this) {
      this.numUnmatchedSequences += numNewUnalignedQueries;
    }
  }

  public long getNumMatchingSequences() {
    return numMatchingSequences;
  }

  public long getNumSequences() {
    return numUnmatchedSequences + numMatchingSequences;
  }

  public long getTotalAlignedQueryLength() {
    return this.totalAlignedQueryLength;
  }

  public double getTotalAlignedPenalty() {
    return this.totalAlignedPenalty;
  }

  public long getNumAlignedQueries() {
    return numMatchingQueries;
  }

  public Distribution getDistanceBetweenQueryComponents() {
    return distanceBetweenQueryComponents;
  }

  long numMatchingSequences = 0;
  long numMatchingQueries = 0;
  long numUnmatchedSequences = 0;
  double totalAlignedPenalty;
  long totalAlignedQueryLength;
  Distribution distanceBetweenQueryComponents = new Distribution();
}
