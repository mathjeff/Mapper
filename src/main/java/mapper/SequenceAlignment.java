package mapper;

import java.util.ArrayList;
import java.util.List;

// A SequenceAlignment says that two sequences resemble each other
// A SequenceAlignment models insertions and deletions
public class SequenceAlignment {
  public SequenceAlignment(AlignedBlock block, AlignmentParameters parameters, boolean referenceReversed) {
    this.sections = new ArrayList<AlignedBlock>(1);
    this.sections.add(block);
    this.computePenalty(parameters);
    this.referenceReversed = referenceReversed;
  }

  public SequenceAlignment(List<AlignedBlock> sections, AlignmentParameters parameters, boolean referenceReversed) {
    this.sections = sections;
    this.computePenalty(parameters);
    this.referenceReversed = referenceReversed;
    this.check();
  }

  private SequenceAlignment() {
  }

  public SequenceAlignment withSequenceA(Sequence sequenceA) {
    if (sequenceA.getLength() != this.getSequenceA().getLength()) {
      throw new IllegalArgumentException("SequenceAlignment.withSequenceA() changing length of sequence from " + this.getSequenceA().getText() + " to " + sequenceA.getText());
    }
    List<AlignedBlock> newSections = new ArrayList<AlignedBlock>(this.sections.size());
    for (int i = 0; i < this.sections.size(); i++) {
      AlignedBlock section = this.sections.get(i);
      newSections.add(this.sections.get(i).withSequenceA(sequenceA));
    }
    SequenceAlignment newAlignment = new SequenceAlignment();
    newAlignment.sections = newSections;
    newAlignment.penalty = this.penalty;
    newAlignment.alignedPenalty = this.alignedPenalty;
    newAlignment.referenceReversed = this.referenceReversed;
    return newAlignment;
  }

  public void check() {
    for (int i = 0; i < this.sections.size() - 1; i++) {
      AlignedBlock prev = this.sections.get(i);
      AlignedBlock next = this.sections.get(i + 1);
      // `next` is an indel
      if (next.getStartIndexA() != prev.getEndIndexA() && next.getEndIndexA() != prev.getStartIndexA()) {
        fail("next start a = " + next.getStartIndexA() + ", prev end a = " + prev.getEndIndexA() + ", next end a = " + next.getEndIndexA() + " prev start a = " + prev.getStartIndexA());
      }
      if (next.getStartIndexB() != prev.getEndIndexB() && next.getEndIndexB() != prev.getStartIndexB()) {
        fail("next start b = " + next.getStartIndexB() + ", prev end b = " + prev.getEndIndexB() + " next end b = " + next.getEndIndexB() + " prev start b = " + prev.getStartIndexB());
      }
    }
  }

  private void fail(String message) {
    throw new IllegalArgumentException(message);
  }

  public List<AlignedBlock> getSections() {
    return this.sections;
  }

  public boolean hasIndel() {
    return this.sections.size() > 1;
  }

  public boolean hasAmbiguousBasepairs() {
    for (AlignedBlock section: this.sections) {
      if (section.hasAmbiguousBasepairs())
        return true;
    }
    return false;
  }

  public int getLengthABefore(int indexB) {
    int total = 0;
    for (AlignedBlock block : this.sections) {
      if (indexB <= block.getStartIndexB()) {
        break;
      }
      if (block.getLengthA() < 1)
        continue;
      if (block.getLengthA() > block.getLengthB()) {
        total += block.getLengthA();
      } else {
        if (indexB < block.getEndIndexB()) {
          total += indexB - block.getStartIndexB();
        } else {
          total += block.getLengthA();
        }
      }
    }
    return total;
  }

  public int getLengthAAfter(int indexB) {
    int total = 0;
    for (AlignedBlock block : this.sections) {
      if (indexB >= block.getEndIndexB()) {
        continue;
      }
      if (block.getLengthA() < 1)
        continue;
      if (block.getLengthA() > block.getLengthB()) {
        total += block.getLengthA();
      } else {
        if (indexB > block.getStartIndexB()) {
          total += block.getEndIndexB() - indexB;
        } else {
          total += block.getLengthA();
        }
      }
    }
    return total;
  }

  public int getNumSections() {
    return this.sections.size();
  }

  public int countNumIndels() {
    int count = 0;
    for (AlignedBlock section: this.sections) {
      if (section.getLengthA() != section.getLengthB())
        count++;
    }
    return count;
  }

  public AlignedBlock getSection(int index) {
    return this.sections.get(index);
  }

  public AlignedBlock getFirstSection() {
    return this.sections.get(0);
  }

  public AlignedBlock getLastSection() {
    return this.sections.get(this.sections.size() - 1);
  }

  public int getStartOffset() {
    return this.sections.get(0).getOffset();
  }

  public int getStartIndexB() {
    return this.sections.get(0).getStartIndexB();
  }

  public int getEndIndexB() {
    return this.sections.get(this.sections.size() - 1).getEndIndexB();
  }

  public int getStartIndexA() {
    return this.sections.get(0).getStartIndexA();
  }

  public int getEndIndexA() {
    return this.sections.get(this.sections.size() - 1).getEndIndexA();
  }

  public int getLengthA() {
    return this.getEndIndexA() - this.getStartIndexA();
  }

  public int getLengthB() {
    return this.getEndIndexB() - this.getStartIndexB();
  }

  private void computePenalty(AlignmentParameters parameters) {
    int alignedQueryLength = 0;
    double totalPenalty = 0;
    for (AlignedBlock block : this.sections) {
      totalPenalty += block.getPenalty(parameters);
      alignedQueryLength += block.getLengthA();
    }
    if (this.sections.size() > 0) {
      AlignedBlock firstBlock = this.sections.get(0);
      if (parameters.StartingInsertionStartFree && firstBlock.getLengthB() == 0)
        totalPenalty -= parameters.InsertionStart_Penalty;
    }
 
    this.alignedPenalty = totalPenalty;
    if (this.sections.size() > 0) {
      AlignedBlock firstBlock = this.sections.get(0);
      int unalignedQueryLength = firstBlock.getSequenceA().getLength() - alignedQueryLength;
      double unalignedPenalty = (double)unalignedQueryLength * parameters.AmbiguityPenalty;
      totalPenalty += unalignedPenalty;
    }
    this.penalty = totalPenalty;
  }

  public double getPenalty() {
    return this.penalty;
  }

  public double getAlignedPenalty() {
    return this.alignedPenalty;
  }

  public double getPenalty(AlignmentParameters parameters, int startIndexB, int endIndexB) {
    double totalPenalty = 0;
    for (AlignedBlock block: this.sections) {
      totalPenalty += block.getPenalty(parameters, startIndexB, endIndexB);
    }
    return totalPenalty;
  }

  public Sequence getSequenceA() {
    return this.sections.get(0).getSequenceA();
  }

  public Sequence getSequenceB() {
    return this.sections.get(0).getSequenceB(); 
  }

  public Sequence getSequenceBHistory() {
    return this.sections.get(0).getSequenceBHistory();
  }

  public String getAlignedTextA() {
    String result = "";
    for (AlignedBlock block: sections) {
      if (block.aLength > 0) {
        result += block.getTextA();
      } else {
        for (int i = 0; i < block.bLength; i++) {
          result += "-";
        }
      }
    }
    return result;
  }

  public String getAlignedTextBHistory() {
    String result = "";
    for (AlignedBlock block: sections) {
      if (block.bLength > 0) {
        result += block.getTextBHistory();
      } else {
        for (int i = 0; i < block.aLength; i++) {
          result += "-";
        }
      }
    }
    return result; 
  }

  public String getAlignedTextB() {
    String result = "";
    for (AlignedBlock block: sections) {
      if (block.bLength > 0) {
        result += block.getTextB();
      } else {
        for (int i = 0; i < block.aLength; i++) {
          result += "-";
        }
      }
    }
    return result;
  }

  // the length of the section of SequenceB that is included in the alignment
  public int getALength() {
    int total = 0;
    for (AlignedBlock block: sections) {
      total += block.aLength;
    }
    return total;
  }

  public boolean isReferenceReversed() {
    return referenceReversed;
  }

  // Just returns the aligned text of the two sequences (using "-" for indels)
  public String format() {
    return this.getAlignedTextA() + "\n" + this.getAlignedTextB();
  }

  // Returns a detailed explanation of what this alignment is
  public String formatVerbose() {
    StringBuilder builder = new StringBuilder();

    SequenceAlignment alignment = this;
    Sequence query = alignment.getSequenceA();

    int alignmentLength = alignment.getALength();
    double penalty = alignment.getPenalty();

    String alignedQuery = alignment.getAlignedTextA();

    String alignedAncestralRef = alignment.getAlignedTextBHistory();
    String alignedUnmutatedRef = alignment.getAlignedTextB();

    String queryText = query.getText();
    String expectedAlignedText = queryText;
    if (alignment.isReferenceReversed()) {
      String originalQueryText = query.reverseComplement().getText();
      builder.append("        Query: " + originalQueryText + "\n");
      builder.append("     RC Query: " + queryText + "\n");
    } else {
      builder.append("        Query: " + queryText + "\n");
    }

    if (!queryText.equals(alignedQuery)) {
      // If printing the aligned query is different from printing the query, then also print
      // the alignment of the query
      builder.append("Aligned query: " + alignedQuery + "\n");
    }
    if (!alignedQuery.equals(alignedAncestralRef)) {
      builder.append("Difference   : ");
      int max = Math.min(alignedQuery.length(), alignedAncestralRef.length());
      for (int i = 0; i < max; i++) {
        char c1 = alignedQuery.charAt(i);
        char c2 = alignedAncestralRef.charAt(i);
        if (c1 == c2) {
          builder.append(" ");
        } else {
          if (Basepairs.canMatch(Basepairs.encode(c1), Basepairs.encode(c2))) {
            builder.append("~");
          } else {
            builder.append("!");
          }
        }
      }
      builder.append("\n");
    }
    if (!alignedAncestralRef.equals(alignedUnmutatedRef)) {
      // If the ancestor analysis had an effect here, explain that too
      builder.append("Ancestral ref: " + alignedAncestralRef + "(" + alignment.getSequenceBHistory().getName() + ", offset " + alignment.getStartOffset() + ")\n");
      builder.append("Original ref : " + alignedUnmutatedRef + "(" + alignment.getSequenceB().getName() + ", offset " + alignment.getStartOffset() + ")\n");
    } else {
      builder.append("Aligned ref  : " + alignedUnmutatedRef + "(" + alignment.getSequenceB().getName() + ", offset " + alignment.getStartOffset() + ")\n");
    }
    builder.append("Penalty      : " + penalty + " (length: " + alignmentLength + ")\n");
    return builder.toString();
  }

  public void putSequenceB(Sequence sequence) {
    if (sequence == null) {
      throw new IllegalArgumentException("putSequenceB(null) for SequenceAlignment:\n" + this.format());
    }
    for (AlignedBlock block: this.sections) {
      block.putSequenceB(sequence);
    }
  }

  public boolean coversIndexB(int index) {
    for (AlignedBlock block: this.sections) {
      if (block.getStartIndexB() <= index && block.getEndIndexB() > index) {
        return true;
      }
    }
    return false;
  }

  public boolean containsSameOffsetAsMatch(SequenceMatch match) {
    if (this.getSequenceB() != match.getSequenceB())
      return false;
    for (AlignedBlock block: this.sections) {
      if (block.getOffset() == match.getOffset())
        return true;
    }
    return false;
  }

  public int getInsertAOrBLength() {
    int total = 0;
    for (AlignedBlock block: this.sections) {
      if (block.getLengthA() != block.getLengthB())
        total += block.getLengthA() + block.getLengthB();
    }
    return total;
  }

  @Override
  public int hashCode() {
    return this.sections.get(0).getOffset();
  }

  @Override
  public boolean equals(Object otherObject) {
    SequenceAlignment other = (SequenceAlignment)otherObject;
    if (this.sections.size() != other.sections.size())
      return false;
    if (this.referenceReversed != other.referenceReversed)
      return false;
    for (int i = 0; i < this.sections.size(); i++) {
      AlignedBlock ourBlock = this.sections.get(i);
      AlignedBlock theirBlock = other.sections.get(i);
      if (!ourBlock.equals(theirBlock)) {
        return false;
      }
    }
    return true;
  }

  private List<AlignedBlock> sections;
  private double penalty;
  private double alignedPenalty;
  private boolean referenceReversed;
}
