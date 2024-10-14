package mapper;

// a SimpleQueryProvider just reads sequences and treats each one as a query
public class SimpleQueryProvider implements QueryProvider {
  public SimpleQueryProvider(SequenceProvider sequenceProvider) {
    this.sequenceProvider = sequenceProvider;
  }

  public QueryBuilder getNextQueryBuilder() {
    SequenceBuilder builder = this.sequenceProvider.getNextSequence();
    if (builder == null) {
      return null;
    }
    return new QueryBuilder(builder);
  }

  public boolean get_allReadsContainQualityInformation() {
    return this.sequenceProvider.get_allReadsContainQualityInformation();
  }

  public boolean get_containsPairedEndReads() {
    return false;
  }

  @Override
  public String toString() {
    return this.sequenceProvider.toString();
  }
  
  private SequenceProvider sequenceProvider;
}
