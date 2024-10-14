package mapper;

import java.util.List;
import java.util.Map;

public class AlignmentCounter implements AlignmentListener {
  public void addAlignments(List<QueryAlignments> alignments) {
    int newNumMatchingSequences = 0;
    int newNumMatchingQueries = 0;
    double newTotalAlignedPenalty = 0;
    long newTotalAlignedQueryLength = 0;
    int numNewUnalignedQuerySequences = 0;
    Distribution newTotalDistanceBetweenComponents = new Distribution();
    for (QueryAlignments queryAlignments : alignments) {
      for (Map.Entry<Query, List<QueryAlignment>> foundAlignments : queryAlignments.getAlignments().entrySet()) {
        List<QueryAlignment> alignment = foundAlignments.getValue();
        Query query = foundAlignments.getKey();
        if (alignment.size() > 0) {
          newNumMatchingQueries++;
          newNumMatchingSequences += query.getNumSequences();
  
          newTotalAlignedPenalty += alignment.get(0).getPenalty();
          newTotalAlignedQueryLength += alignment.get(0).getALength();
  
          double currentTotalDistanceBetweenComponents = 0;
          for (QueryAlignment choice: alignment) {
            newTotalDistanceBetweenComponents.add(choice.getTotalDistanceBetweenComponents(), (double)1.0 / (double)alignment.size());
          }
        } else {
          numNewUnalignedQuerySequences += query.getNumSequences();
        }

      }
    }
    synchronized (this) {
      this.numMatchingSequences += newNumMatchingSequences;
      this.numMatchingQueries += newNumMatchingQueries;
      this.totalAlignedPenalty += newTotalAlignedPenalty;
      this.totalAlignedQueryLength += newTotalAlignedQueryLength;
      this.distanceBetweenQueryComponents = this.distanceBetweenQueryComponents.plus(newTotalDistanceBetweenComponents);
      this.numUnmatchedSequences += numNewUnalignedQuerySequences;
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
