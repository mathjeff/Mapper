package mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// Summarizes the penalties of alignments into a histogram
public class IndelSummarizer implements AlignmentListener {
  public IndelSummarizer() {
    this.extensionCounts = new ArrayList<Integer>();
  }

  public void addAlignments(List<QueryAlignments> queryAlignments) {
    ArrayList<Integer> additions = new ArrayList<Integer>();

    for (QueryAlignments alignments: queryAlignments) {
      for (Map.Entry<Query, List<QueryAlignment>> foundAlignments: alignments.getAlignments().entrySet()) {
        List<QueryAlignment> alignment = foundAlignments.getValue();
        Query query = foundAlignments.getKey();
        if (alignment.size() > 0) {
          QueryAlignment firstAlignment = alignment.get(0);
          for (SequenceAlignment component: firstAlignment.getComponents()) {
            for (AlignedBlock block: component.getSections()) {
              int indelLength = block.getIndelLength();
              if (indelLength > 0) {
                while (additions.size() <= indelLength) {
                  additions.add(0);
                }
                additions.set(indelLength, additions.get(indelLength) + 1);
              }
            }
          }
        }
      }
    }
    addIndels(additions);
  }

  private void addIndels(ArrayList<Integer> indelLengths) {
    synchronized(this) {
      while (this.extensionCounts.size() < indelLengths.size()) {
        this.extensionCounts.add(0);
      }
      for (int i = 0; i < indelLengths.size(); i++) {
        this.extensionCounts.set(i, this.extensionCounts.get(i) + indelLengths.get(i));
      }
    }
  }

  public double[] getInterestingIndelLengthCounts() {
    // compute total
    double total = 0;
    for (int i = 0; i < this.extensionCounts.size(); i++) {
      total += this.extensionCounts.get(i);
    }
    // find max length that is a significant fraction of the total
    int maxInterestingLength = 0;
    for (int i = 0; i < this.extensionCounts.size(); i++) {
      if (this.extensionCounts.get(i) * 100 >= total)
        maxInterestingLength = i + 1;
    }
    // also show the next length so the caller can know that we have more data
    if (maxInterestingLength + 1 < this.extensionCounts.size()) {
      maxInterestingLength++;
    }

    // return results
    double[] results = new double[maxInterestingLength];
    for (int i = 0; i < results.length; i++) {
      results[i] = this.extensionCounts.get(i);
    }
    return results;
  }

  ArrayList<Integer> extensionCounts;
}
