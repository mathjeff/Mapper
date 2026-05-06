package mapper;

import java.util.ArrayList;
import java.util.List;

public class AlignmentParameters {
  // the penalty we apply for a point mutation
  public double MutationPenalty;

  // the penalty we apply for starting an insertion (base pairs in the query that don't match the reference)
  public double InsertionStart_Penalty;
  // the penalty we apply for extending an insertion by 1 base pair
  public double InsertionExtension_Penalty;

  // the penalty we apply for starting a deletion (base pairs in the reference that don't match the query)
  public double DeletionStart_Penalty;
  // the penalty we apply for extending a deletion by 1 base pair
  public double DeletionExtension_Penalty;

  // The cutoff for how different the sequences can be: the penalty divided by sequence length
  public double MaxErrorRate;

  // The amount of penalty added by an unaligned base pair
  public double UnalignedPenalty;

  // The amount of penalty added by an ambiguous base pair ('N')
  public double AmbiguityPenalty;

  // The maximum number of places that a sequence can match before we ignore it
  public int MaxNumMatches = Integer.MAX_VALUE;

  // The maximum difference in penalty between the lowest-penalty alignment that we report and the highest-penalty alignment that we report
  public double Max_PenaltySpan;

  public boolean StartingInsertionStartFree;
  public double getStartingInsertionStartPenalty() {
    if (StartingInsertionStartFree)
      return 0;
    return InsertionStart_Penalty;
  }

  public double getMinPossibleNonzeroPenalty() {
    double result = this.MutationPenalty;
    result = Math.min(result, this.getStartingInsertionStartPenalty() + this.InsertionStart_Penalty);
    result = Math.min(result, this.DeletionStart_Penalty + this.DeletionExtension_Penalty);
    return result;
  }

  public AlignmentParameters clone() {
    AlignmentParameters result = new AlignmentParameters();

    result.MutationPenalty = MutationPenalty;
    result.InsertionStart_Penalty = InsertionStart_Penalty;
    result.InsertionExtension_Penalty = InsertionExtension_Penalty;
    result.DeletionStart_Penalty = DeletionStart_Penalty;
    result.DeletionExtension_Penalty = DeletionExtension_Penalty;
    result.MaxErrorRate = MaxErrorRate;
    result.UnalignedPenalty = UnalignedPenalty;
    result.AmbiguityPenalty = AmbiguityPenalty;
    result.MaxNumMatches = MaxNumMatches;
    result.Max_PenaltySpan = Max_PenaltySpan;
    result.StartingInsertionStartFree = StartingInsertionStartFree;

    return result;
  }

  public SequenceAlignment newSequenceAlignment(AlignedBlock block, boolean referenceReversed) {
    List<AlignedBlock> blocks = new ArrayList<AlignedBlock>(1);
    blocks.add(block);
    return newSequenceAlignment(blocks, referenceReversed);
  }

  public SequenceAlignment newSequenceAlignment(List<AlignedBlock> sections, boolean referenceReversed) {
    int alignedQueryLength = 0;
    double totalPenalty = 0;
    for (AlignedBlock block : sections) {
      totalPenalty += this.getPenalty(block);
      alignedQueryLength += block.getLengthA();
    }
    if (sections.size() > 0) {
      AlignedBlock firstBlock = sections.get(0);
      if (this.StartingInsertionStartFree && firstBlock.getLengthB() == 0)
        totalPenalty -= this.InsertionStart_Penalty;
    }

    double alignedPenalty = totalPenalty;
    if (sections.size() > 0) {
      AlignedBlock firstBlock = sections.get(0);
      int unalignedQueryLength = firstBlock.getSequenceA().getLength() - alignedQueryLength;
      double unalignedPenalty = (double)unalignedQueryLength * this.UnalignedPenalty;
      totalPenalty += unalignedPenalty;
    }
    double penalty = totalPenalty;
    return new SequenceAlignment(sections, referenceReversed, totalPenalty, alignedPenalty);
  }

  public double getPenalty(SequenceAlignment alignment, int startIndexB, int endIndexB) {
    double totalPenalty = 0;
    for (AlignedBlock block: alignment.getSections()) {
      totalPenalty += this.getPenalty(block, startIndexB, endIndexB);
    }
    return totalPenalty;
  }


  public double getPenalty(AlignedBlock block) {
    double penalty = 0;
    if (block.getLengthA() == block.getLengthB()) {
      // this block is a 1-to-1 matching
      for (int i = 0; i < block.getLengthA(); i++) {
        byte a = block.getSequenceA().encodedCharAt(block.getStartIndexA() + i);
        byte b = block.getSequenceBHistory().encodedCharAt(block.getStartIndexB() + i);
        penalty += this.getPenalty(a, b);
      }
    } else {
      // this block is an insertion or a deletion
      if (block.getLengthA() > 0) {
        penalty += this.InsertionStart_Penalty;
        penalty += this.InsertionExtension_Penalty * block.getLengthA();
      } else {
        penalty += this.DeletionStart_Penalty;
        penalty += this.DeletionExtension_Penalty * block.getLengthB();
      }
    }
    return penalty;
  }

  public double getPenalty(AlignedBlock block, int startIndexB, int endIndexB) {
    double penalty = 0;
    if (block.getLengthA() == block.getLengthB()) {
      // this block is a 1-1 matching
      for (int i = 0; i < block.getLengthA(); i++) {
        int bIndex = block.getStartIndexB() + i;
        if (bIndex >= startIndexB && bIndex < endIndexB) {
          int aIndex = block.getStartIndexA() + i;
          byte a = block.getSequenceA().encodedCharAt(aIndex);
          byte b = block.getSequenceBHistory().encodedCharAt(bIndex);
          penalty += this.getPenalty(a, b);
        }
      }
    } else {
      // this block is an insertion or a deletion
      if (block.getStartIndexB() < endIndexB && block.getEndIndexB() > startIndexB) {
        if (block.getLengthA() > 0) {
          penalty += this.InsertionStart_Penalty;
          penalty += this.InsertionExtension_Penalty * block.getLengthA();
        } else {
          penalty += this.DeletionStart_Penalty;
          penalty += this.DeletionExtension_Penalty * block.getLengthB();
        }
      }
    }
    return penalty;
  }

  public double getPenalty(byte encodedQuery, byte encodedReference) {
    // The penalty of an alignment is essentially the log of how likely we think it is to observe this alignment
    if (!Basepairs.canMatch(encodedReference, encodedQuery)) {
      // if the two basepairs don't match, that's a mutation, and we know the penalty
      return this.MutationPenalty;
    }
    // if the two basepairs can match, we have to calculate a penalty if there is ambiguity
    // We want to calculate the probability that we could identify a mutation in one of these basepairs

    // Suppose that the possible values reported for the query must be one of:
    //  - The ambiguity code we have for it now
    //  - Any other specific basepair that doesn't match that ambiguity code
    // Suppose that the possible values reported for the reference must be one of:
    //  - The ambiguity code we have for it now
    //  - Any other specific basepair that doesn't match that ambiguity code

    // This means:
    //  - If the query's true value mutates into another specific basepair covered by its current ambiguity code, we expect it to be still reported via the same ambiguity code and we expect to not be able to detect this mutation
    //  - If the query's true value mutates into another specific basepair covered by the reference's ambiguity code, we would still consider this to be a match and wouldn't notice the mutation
    //  - If the query's true value mutates into another specific basepair that isn't covered by the query's current ambiguity code or the reference's current ambiguity code, then we could detect this mutation

    // So, we want to compute the probability that a mutation in one of these basepairs would change it into a basepair that isn't included in the either the query's current ambiguity code or the reference's ambiguity code
    byte encodedUnion = Basepairs.union(encodedQuery, encodedReference);
    return this.AmbiguityPenalty * Basepairs.getMutationFalseNegativeRate(encodedUnion);
  }

}
