package mapper;

import java.util.List;
import java.util.Map;

// Summarizes the penalties of alignments into a histogram
public class PenaltySummarizer implements AlignmentListener {
  public PenaltySummarizer(AlignmentParameters alignmentParameters) {
    this.counts = new int[20];
    this.alignmentParameters = alignmentParameters;
  }

  public void addAlignments(List<QueryAlignments> queryAlignments) {
    int[] additions = new int[this.counts.length];
    for (QueryAlignments alignments: queryAlignments) {
      for (int i = 0; i < alignments.getNumComponents(); i++) {
        List<QueryAlignment> alignment = alignments.getAlignments(i);

        if (alignment.size() > 0) {
          QueryAlignment firstAlignment = alignment.get(0);
          double penalty = firstAlignment.getPenalty();
          int queryLength = alignments.getQueryLength(i);
          double maxAllowedPenalty = queryLength * this.alignmentParameters.MaxErrorRate;
          if (maxAllowedPenalty == 0)
            maxAllowedPenalty = 1;
          double penaltyFraction = penalty / maxAllowedPenalty;
          int binIndex = (int)((double)penaltyFraction * (double)this.counts.length);
          if (binIndex < additions.length)
            additions[binIndex]++;
        }
      }
    }
    this.add(additions);
  }

  public double[] getCounts() {
    double[] results = new double[this.counts.length];
    for (int i = 0; i < this.counts.length; i++) {
      results[i] = this.counts[i];
    }
    return results;
  }

  private void add(int[] additions) {
    synchronized(this) {
      for (int i = 0; i < additions.length; i++) {
        this.counts[i] += additions[i];
      }
    }
  }

  int[] counts;
  AlignmentParameters alignmentParameters;
}
