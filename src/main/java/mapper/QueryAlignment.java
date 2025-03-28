package mapper;

import java.util.ArrayList;
import java.util.List;

// A QueryAlignment tells where a Query aligns
// Its like a SequenceAlignment but the query might consist of multiple sequences if it is two Illumina-style paired-end reads
public class QueryAlignment {

  public QueryAlignment(SequenceAlignment sequenceAlignment) {
    this.alignments = new ArrayList<SequenceAlignment>(1);
    this.alignments.add(sequenceAlignment);
    this.totalPenalty = sequenceAlignment.getPenalty();
  }

  public QueryAlignment(List<SequenceAlignment> sequenceAlignments, double spacingPenalty, double overlapMultiplier, double duplicationBonus, double totalPenalty, int totalDistanceBetweenComponents) {
    this.alignments = sequenceAlignments;
    this.spacingPenalty = spacingPenalty;
    this.overlapMultiplier = overlapMultiplier;
    this.duplicationBonus = duplicationBonus;
    this.totalPenalty = totalPenalty;
    this.totalDistanceBetweenComponents = totalDistanceBetweenComponents;
  }

  public QueryAlignment withQuery(Query query) {
    List<SequenceAlignment> newComponents = new ArrayList<SequenceAlignment>(this.alignments.size());
    for (int i = 0; i < this.alignments.size(); i++) {
      Sequence newSequenceA = query.getSequence(i);
      SequenceAlignment existingAlignment = this.alignments.get(i);
      if (existingAlignment.isReferenceReversed()) {
        newSequenceA = newSequenceA.reverseComplement();
      }
      newComponents.add(existingAlignment.withSequenceA(newSequenceA));
    }
    return new QueryAlignment(newComponents, this.spacingPenalty, this.overlapMultiplier, this.duplicationBonus, this.totalPenalty, this.totalDistanceBetweenComponents);
  }

  // list of alignments for each sequence
  public List<SequenceAlignment> getComponents() {
    return alignments;
  }

  public SequenceAlignment getComponent(int index) {
    return alignments.get(index);
  }

  public Sequence getSequenceB() {
    SequenceAlignment firstComponent = this.getComponent(0);
    return firstComponent.getSection(0).getSequenceB();
  }

  public void putSequenceB(Sequence sequence) {
    for (SequenceAlignment component: this.alignments) {
      component.putSequenceB(sequence);
    }
  }

  public int getNumSequences() {
    return this.alignments.size();
  }

  public double getSpacingPenalty() {
    return this.spacingPenalty;
  }

  public double getOverlapMultiplier() {
    return this.overlapMultiplier;
  }

  public double getDuplicationBonus() {
    return this.duplicationBonus;
  }

  public double getPenalty() {
    return this.totalPenalty;
  }

  public int getALength() {
    int total = 0;
    for (SequenceAlignment component: this.alignments) {
      total += component.getALength();
    }
    return total;
  }

  // Returns the total inner distance between subsequent pairs of components
  public int getTotalDistanceBetweenComponents() {
    return totalDistanceBetweenComponents;
  }

  public int getNumAlignmentsCoveringIndexB(int referenceIndex) {
    if (this.minOverlap < 0)
      this.computeOverlap();
    if (referenceIndex < this.minOverlap)
      return 1;
    if (referenceIndex >= this.maxOverlap)
      return 1;
    return this.alignments.size();
  }

  public String formatQuery() {
    String result = "";
    for (SequenceAlignment alignment: this.alignments) {
      if (result.length() > 0) {
        result += " / ";
      }
      result += alignment.getSequenceA().getText();
    }
    return result;
  }

  // returns a short description of what this alignment is: where it is and where any indels are
  public String format() {
    String result = "";
    for (int i = 0; i < this.alignments.size(); i++) {
      SequenceAlignment alignment = this.alignments.get(i);
      if (this.alignments.size() > 1)
        result += "component " + i + ":\n";
      result += "alignment at " + alignment.getSequenceB().getName() + " offset " + alignment.getStartOffset() + ":\n";
      result += alignment.format();
      result += "\n";
    }
    return result;
  }

  // returns a detailed description of what this alignment is
  public String formatVerbose() {
    StringBuilder builder = new StringBuilder();
    if (this.getNumSequences() > 1) {
      builder.append("Total penalty: " + this.getPenalty() + ": " + this.explainPenalty() + "\n");
    }
    for (SequenceAlignment component: this.getComponents()) {
      builder.append(component.formatVerbose());
    }
    return builder.toString();
  }

  public boolean containsSameOffsetAsMatch(QueryMatch match) {
    for (int i = 0; i < match.getNumSequences(); i++) {
      SequenceMatch sequenceMatch = match.getComponents().get(i);
      SequenceAlignment sequenceAlignment = this.alignments.get(i);
      if (!sequenceAlignment.containsSameOffsetAsMatch(sequenceMatch))
        return false;
    }
    return true;
  }

  public String explainPenalty() {
    StringBuilder resultBuilder = new StringBuilder();
    resultBuilder.append("(");
    for (int i = 0; i < this.alignments.size(); i++) {
      SequenceAlignment component = this.alignments.get(i);
      resultBuilder.append("seq" + (i + 1) + " penalty (" + component.getPenalty() + ")");
      if (i != this.alignments.size() - 1) {
        resultBuilder.append(" + ");
      }
    }
    resultBuilder.append(" - duplicated penalty (" + this.duplicationBonus + ")");
    resultBuilder.append(") * (total length / unique length) (" + this.overlapMultiplier + ")");
    resultBuilder.append(" + spacing penalty (" + this.spacingPenalty + ")");
    return resultBuilder.toString();
  }

  public boolean hasIndel() {
    for (SequenceAlignment component : this.alignments) {
      if (component.hasIndel())
        return true;
    }
    return false;
  }

  public boolean hasAmbiguousBasepairs() {
    for (SequenceAlignment component: this.alignments) {
      if (component.hasAmbiguousBasepairs())
        return true;
    }
    return false;
  }

  private double round(double value, double scale) {
    return Math.round(value * scale) / scale;
  }

  private void computeOverlap() {
    for (SequenceAlignment alignment: this.alignments) {
      int min = alignment.getStartIndexB();
      int max = alignment.getEndIndexB();
      if (this.minOverlap < 0 || min >= this.minOverlap) {
        this.minOverlap = min;
      }
      if (this.maxOverlap < 0 || max <= this.maxOverlap) {
        this.maxOverlap = max;
      }
    }
  }

  // returns the distance between the two blocks
  private int getDistance(AlignedBlock a, AlignedBlock b) {
    if (a.getSequenceB() != b.getSequenceB())
      return Integer.MAX_VALUE;
    if (b.getSequenceB().getComplementedFrom() != null) {
      throw new IllegalArgumentException("QueryAlignment.getDistance with a reversed: " + a.getSequenceB().getName());
    }
    return b.getStartIndexB() - a.getEndIndexB();
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (SequenceAlignment alignment: this.alignments) {
      hash = hash * 1001 + alignment.hashCode();
    }
    return hash;
  }

  @Override
  public boolean equals(Object otherObject) {
    QueryAlignment other = (QueryAlignment)otherObject;
    if (this.spacingPenalty != other.spacingPenalty)
      return false;
    if (this.overlapMultiplier != other.overlapMultiplier)
      return false;
    if (this.duplicationBonus != other.duplicationBonus)
      return false;
    if (this.totalPenalty != other.totalPenalty)
      return false;
    if (this.totalDistanceBetweenComponents != other.totalDistanceBetweenComponents)
      return false;
    if (other.alignments.size() != this.alignments.size())
      return false;
    for (int i = 0; i < this.alignments.size(); i++) {
      if (!this.alignments.get(i).equals(other.alignments.get(i)))
        return false;
    }
    return true;
  }

  private List<SequenceAlignment> alignments;
  // The penalty caused by the spacing between the sequence alignments
  private double spacingPenalty;

  private int minOverlap = -1;
  private int maxOverlap = -1;

  // penalty due to components overlapping
  private double overlapMultiplier;
  // penalty that appears in each component and shouldn't be counted twice
  private double duplicationBonus;
  private double totalPenalty;
  private int totalDistanceBetweenComponents;
}
