package mapper;

public interface QueryProvider {
  QueryBuilder getNextQueryBuilder();
  boolean get_allReadsContainQualityInformation();
  boolean get_containsPairedEndReads();
}
