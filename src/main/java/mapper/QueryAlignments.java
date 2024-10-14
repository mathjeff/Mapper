package mapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// A QueryAlignments lists the places where a Query may align
// It's mostly a List<QueryAlignment> with some extra metadata about whether we were able to align each Sequence
public class QueryAlignments {

  public QueryAlignments(Query query, QueryAlignment alignment) {
    this.alignments = new LinkedHashMap<Query, List<QueryAlignment>>(1);
    List<QueryAlignment> queryAlignments = new ArrayList<QueryAlignment>(1);
    queryAlignments.add(alignment);
    this.alignments.put(query, queryAlignments);
  }

  public QueryAlignments(Map<Query, List<QueryAlignment>> alignments) {
    this.alignments = alignments;
  }

  public QueryAlignments(Query query, List<QueryAlignment> alignments) {
    this.alignments = new LinkedHashMap<Query, List<QueryAlignment>>(1);
    this.alignments.put(query, alignments);
  }

  public QueryAlignments(Query query) {
    this.alignments = new LinkedHashMap<Query, List<QueryAlignment>>(1);
    this.alignments.put(query, new ArrayList<QueryAlignment>(0));
  }

  public Map<Query, List<QueryAlignment>> getAlignments() {
    return this.alignments;
  }

  public List<QueryAlignment> getAlignmentsForQuery(Query query) {
    List<QueryAlignment> result = this.alignments.get(query);
    if (result == null)
      result = new ArrayList<QueryAlignment>(0);
    return result;
  }

  public int getTotalOfAllComponents() {
    int total = 0;
    for (List<QueryAlignment> value: this.alignments.values()) {
      total += value.size();
    }
    return total;
  }

  // Returns the number of subqueries that this alignment represents
  // If our query wasn't a paired-end read, this number should be 1
  // If our query was a paired-end read:
  //  If neither mate aligned, this number should be 1
  //  If both mates aligned together, this number should be 1
  //  If one query aligned and one didn't, this number should be 2
  //  If both aligned to different places, this number should be 2
  public int getNumQueries() {
    return alignments.size();
  }

  // Returns the number of queries for which we found an alignment.
  // If our query wasn't a paired-end read:
  //  this number should be 0 (unaligned) or 1 (aligned)
  // If our query was a paired-end read:
  //  If neither mate aligned anywhere, this should return 0
  //  If both mates aligned together, this should return 1
  //  If one mate aligned and one didn't, this should return 1
  //  If both mates aligned but not together, this should return 2
  public int getNumQueriesHavingAlignments() {
    int count = 0;
    for (Map.Entry<Query, List<QueryAlignment>> entry: this.alignments.entrySet()) {
      if (entry.getValue().size() > 0) {
        count++;
      }
    }
    return count;
  }

  public Query getFirstQuery() {
    for (Query query: this.alignments.keySet()) {
      return query;
    }
    return null;
  }

  public List<QueryAlignment> getFirstAlignments() {
    for (List<QueryAlignment> components: this.alignments.values()) {
      return components;
    }
    return null;
  }

  private Map<Query, List<QueryAlignment>> alignments;
}
