package mapper;

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
}
