package mapper;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class MatchDatabase_Test {
  public MatchDatabase_Test() {
  }

  @Test
  public void testQueryEndingWithMismatch() {
    String queryText = "AACCACGT";
    String refText =   "AACCACGA";

    Sequence a = new SequenceBuilder().setName("a").add(queryText).build();
    Query query = new Query(a);
    Sequence b = new SequenceBuilder().setName("b").add(refText).build();
    SequenceAlignment sequenceAlignment = new SequenceAlignment(new AlignedBlock(a, b, 0, 0, queryText.length(), refText.length()), new AlignmentParameters(), false);
    QueryAlignment alignment = new QueryAlignment(sequenceAlignment);
    MatchDatabase database = new MatchDatabase(0);
    List<QueryAlignments> queryAlignments = new ArrayList<QueryAlignments>();
    queryAlignments.add(new QueryAlignments(query, alignment));
    database.addAlignments(queryAlignments);
    Alignments alignments = database.groupByPosition().get(b);
    for (int i = 0; i < refText.length(); i++) {
      AlignmentPosition position = alignments.getPosition(i);
      float count = position.getCount();
      if (count != 1) {
        fail("number of bases aligned at position " + i + " in reference equals " + count + " rather than 1");
      }
    }
  }

  @Test
  public void testOverlappingPairedEndQueries() {
    String refText      = "AACCACGATTAC";
    String query1Text   = "AACCACGA";
    String query2Text   =    "CACGATTAC";
    Sequence query1 = new SequenceBuilder().setName("q1").add(query1Text).build();
    Sequence query2 = new SequenceBuilder().setName("q2").add(query2Text).build();
    Sequence ref = new SequenceBuilder().setName("ref").add(refText).build();

    SequenceAlignment sequence1Alignment = new SequenceAlignment(new AlignedBlock(query1, ref, 0, 0, query1Text.length(), query1Text.length()), new AlignmentParameters(), false);
    SequenceAlignment sequence2Alignment = new SequenceAlignment(new AlignedBlock(query2, ref, 0, 3, query2Text.length(), query2Text.length()), new AlignmentParameters(), false);

    List<SequenceAlignment> components = new ArrayList<SequenceAlignment>();
    components.add(sequence1Alignment);
    components.add(sequence2Alignment);
    QueryAlignment alignment = new QueryAlignment(components, 0, 0, 0, 0, -5);

    MatchDatabase database = new MatchDatabase(0);
    Query query = new Query(query1, query2, 0, 1);
    List<QueryAlignments> queryAlignments = new ArrayList<QueryAlignments>();
    queryAlignments.add(new QueryAlignments(query, alignment));
    database.addAlignments(queryAlignments);

    Alignments alignments = database.groupByPosition().get(ref);
    for (int i = 0; i < refText.length(); i++) {
      AlignmentPosition position = alignments.getPosition(i);
      float count = position.getCount();
      if (count != 1) {
        fail("number of bases aligned at position " + i + " in reference equals " + count + " rather than 1");
      }
    }
 
  }

  private void fail(String message) {
    Assert.fail(message);
  }
}
