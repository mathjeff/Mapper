package mapper;

import java.util.List;

// A QueriesIterator provides a list of queries
public class QueriesIterator implements QueryProvider {
  public QueriesIterator(List<QueryProvider> providers) {
    this.providers = providers;
  }

  public QueryBuilder getNextQueryBuilder() {
    while (this.nextIndex < this.providers.size()) {
      QueryBuilder next = this.providers.get(this.nextIndex).getNextQueryBuilder();
      if (next != null) {
        return next;
      }
      this.nextIndex++;
    }
    return null;
  }

  public boolean get_allReadsContainQualityInformation() {
    for (QueryProvider provider : this.providers) {
      if (!provider.get_allReadsContainQualityInformation()) {
        return false;
      }
    }
    return true;
  }

  public boolean get_containsPairedEndReads() {
    for (QueryProvider provider : this.providers) {
      if (provider.get_containsPairedEndReads()) {
        return true;
      }
    }
    return false;
  }

  int nextIndex;
  List<QueryProvider> providers;
}
