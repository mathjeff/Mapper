package mapper;

import java.util.ArrayList;
import java.util.List;

public class QueryBuilder {

  public QueryBuilder(SequenceBuilder sequenceProvider) {
    this.sequenceProviders = new ArrayList<SequenceBuilder>(1);
    this.sequenceProviders.add(sequenceProvider);
  }

  public QueryBuilder(List<SequenceBuilder> sequenceProviders, double expectedInnerDistance, double spacingDeviationPerUnitPenalty) {
    this.sequenceProviders = sequenceProviders;
    this.maxOffset = maxOffset;
    this.expectedInnerDistance = expectedInnerDistance;
    this.spacingDeviationPerUnitPenalty = spacingDeviationPerUnitPenalty;
  }

  public Query build() {
    List<Sequence> sequences = new ArrayList<Sequence>(this.sequenceProviders.size());
    boolean odd = false;
    for (SequenceBuilder sequenceProvider : this.sequenceProviders) {
      sequences.add(sequenceProvider.build());
    }
    return new Query(sequences, this.expectedInnerDistance, this.spacingDeviationPerUnitPenalty);
  }

  public void setId(long id) {
    for (SequenceBuilder builder : this.sequenceProviders) {
      builder.setId(id);
    }
  }

  public int getLength() {
    int total = 0;
    for (SequenceBuilder builder : this.sequenceProviders) {
      total += builder.getLength();
    }
    return total;
  }

  private List<SequenceBuilder> sequenceProviders;
  private int maxOffset;
  private int id;
  private double expectedInnerDistance;
  private double spacingDeviationPerUnitPenalty;
}
