package mapper;

import java.util.List;

// an AlignmentListener listens for Alignments
public interface AlignmentListener {
  void addAlignments(List<List<QueryAlignment>> alignments);
  void addUnaligned(List<Query> unalignedQueries);
}
