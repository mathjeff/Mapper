package mapper;

import java.util.ArrayList;
import java.util.List;

// A PairedEndQueryProvider generates Query objects from Illumina-style paired-end reads
public class PairedEndQueryProvider implements QueryProvider {
  public PairedEndQueryProvider(SequenceProvider leftsProvider, SequenceProvider rightsProvider, double expectedInnerDistance, double spacingDeviationPerUnitPenalty) {
    this.sequenceProviders = new ArrayList<SequenceProvider>(1);
    this.sequenceProviders.add(leftsProvider);
    this.sequenceProviders.add(rightsProvider);
    this.expectedInnerDistance = expectedInnerDistance;
    this.spacingDeviationPerUnitPenalty = spacingDeviationPerUnitPenalty;
  }

  public QueryBuilder getNextQueryBuilder() {
    List<SequenceBuilder> components = new ArrayList<SequenceBuilder>(this.sequenceProviders.size());
    boolean anyNull = false;
    for (SequenceProvider provider : this.sequenceProviders) {
      SequenceBuilder builder = provider.getNextSequence();
      if (builder == null) {
        anyNull = true;
      }
      components.add(builder);
    }
    if (anyNull) {
      if (components.get(0) == null && components.get(1) == null) {
        // both query providers ended after the same number of queries
        return null;
      }
      // one query provider ended and the other didn't
      int nullIndex;
      if (components.get(0) == null)
        nullIndex = 0;
      else
        nullIndex = 1;
      int nonNullIndex = 1 - nullIndex;
      SequenceProvider completedQueries = sequenceProviders.get(nullIndex);
      SequenceProvider remainingQueries = sequenceProviders.get(nonNullIndex);

      throw new IllegalArgumentException("" + remainingQueries + " has more queries than " + completedQueries + "!");
    }
    // Choose an upper bound on the maximum possible offset between sequences
    return new QueryBuilder(components, this.expectedInnerDistance, this.spacingDeviationPerUnitPenalty);
  }

  @Override
  public String toString() {
    return "paired queries: " + this.sequenceProviders.get(0).toString() + ", " + this.sequenceProviders.get(1).toString();
  }

  public boolean get_allReadsContainQualityInformation() {
    for (SequenceProvider sequenceProvider : this.sequenceProviders) {
      if (!sequenceProvider.get_allReadsContainQualityInformation()) {
        return false;
      }
    }
    return true;
  }

  public boolean get_containsPairedEndReads() {
    return true;
  }

  private List<SequenceProvider> sequenceProviders;
  private double expectedInnerDistance;
  private double spacingDeviationPerUnitPenalty;
}
