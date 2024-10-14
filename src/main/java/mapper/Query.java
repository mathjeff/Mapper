package mapper;

import java.util.ArrayList;
import java.util.List;

// A Query is a question that can be asked about where some sequences align
// It can model a single sequence or can model Illumina-style paired-end reads
public class Query {
  public Query(Sequence sequence) {
    this.sequences = new ArrayList<Sequence>(1);
    this.sequences.add(sequence);
    this.spacingDeviationPerUnitPenalty = 1;
  }

  public Query(Sequence forward, Sequence reverse, double expectedInnerDistance, double spacingDeviationPerUnitPenalty) {
    this.sequences = new ArrayList<Sequence>(2);
    this.sequences.add(forward);
    this.sequences.add(reverse);
    this.expectedInnerDistance = expectedInnerDistance;
    this.spacingDeviationPerUnitPenalty = spacingDeviationPerUnitPenalty;
  }

  public Query(List<Sequence> sequences, double expectedInnerDistance, double spacingDeviationPerUnitPenalty) {
    this.sequences = sequences;
    this.maxOffset = maxOffset;
    this.expectedInnerDistance = expectedInnerDistance;
    this.spacingDeviationPerUnitPenalty = spacingDeviationPerUnitPenalty;
  }

  public Query subquery(int index) {
    Query subquery = new Query(this.sequences.get(index));
    subquery.expectedInnerDistance = this.expectedInnerDistance;
    subquery.spacingDeviationPerUnitPenalty = this.spacingDeviationPerUnitPenalty;
    return subquery;
  }

  public List<Sequence> getSequences() {
    return this.sequences;
  }

  public Sequence getSequence(int index) {
    return this.sequences.get(index);
  }

  public int getNumSequences() {
    return this.sequences.size();
  }

  public double getExpectedInnerDistance() {
    return this.expectedInnerDistance;
  }

  public double getSpacingDeviationPerUnitPenalty() {
    return this.spacingDeviationPerUnitPenalty;
  }

  public long getId() {
    return this.sequences.get(0).getId();
  }

  public int getLength() {
    int total = 0;
    for (Sequence sequence: this.sequences) {
      total += sequence.getLength();
    }
    return total;
  }

  public String format() {
    int totalSize = 0;
    for (Sequence sequence : this.sequences) {
       totalSize += sequence.getLength();
    }
    if (totalSize > 1000) {
      return "[" + this.sequences.size() + " sequences totalling " + totalSize + " base pairs]";
    }

    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < this.sequences.size(); i++) {
      Sequence sequence = this.sequences.get(i);
      builder.append(sequence.getText());
      if (i < this.sequences.size() - 1) {
        builder.append(" / ");
      }
    }
    return builder.toString();
  }

  public void compress() {
    for (Sequence sequence: this.sequences) {
      sequence.compress();
    }
  }

  public void decompress() {
    for (Sequence sequence: this.sequences) {
      sequence.decompress();
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    for (Sequence sequence : this.sequences) {
      builder.append(sequence.getText() + " ");
    }
    return builder.toString();
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (Sequence sequence: sequences) {
      hash *= 13;
      hash += sequence.getContentHash();
    }
    return hash;
  }

  @Override
  public boolean equals(Object otherObject) {
    Query other = (Query)otherObject;
    if (other.sequences.size() != sequences.size())
      return false;
    for (int i = 0; i < sequences.size(); i++) {
      if (!sequences.get(i).textEquals(other.sequences.get(i)))
        return false;
    }
    if (maxOffset != other.maxOffset)
      return false;
    if (expectedInnerDistance != other.expectedInnerDistance)
      return false;
    if (spacingDeviationPerUnitPenalty != other.spacingDeviationPerUnitPenalty)
      return false;
    //System.err.println("Equal queries: " + this.toString() + " and " + other.toString());
    return true;
  }

  private List<Sequence> sequences;
  private int maxOffset; // max distance betweeen where the sequences can be aligned
  private double expectedInnerDistance;
  private double spacingDeviationPerUnitPenalty;
}
