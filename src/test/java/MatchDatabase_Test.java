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
    Sequence b = new SequenceBuilder().setName("b").add(refText).build();
    SequenceAlignment sequenceAlignment = new SequenceAlignment(new AlignedBlock(a, b, 0, 0, queryText.length(), refText.length()), new AlignmentParameters(), false);
    QueryAlignment alignment = new QueryAlignment(sequenceAlignment);
    MatchDatabase database = new MatchDatabase(0);
    List<QueryAlignment> alignmentList = new ArrayList<QueryAlignment>();
    alignmentList.add(alignment);
    List<List<QueryAlignment>> alignmentListList = new ArrayList<List<QueryAlignment>>();
    alignmentListList.add(alignmentList);
    database.addAlignments(alignmentListList);
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
    String query2RCText =    "CACGATTAC";
    Sequence query1 = new SequenceBuilder().setName("q1").add(query1Text).build();
    Sequence query2 = new SequenceBuilder().setName("q1").add(query2RCText).build().reverseComplement();
    Sequence ref = new SequenceBuilder().setName("ref").add(refText).build();

    SequenceAlignment sequence1Alignment = new SequenceAlignment(new AlignedBlock(query1, ref, 0, 0, query1Text.length(), query1Text.length()), new AlignmentParameters(), false);
    SequenceAlignment sequence2Alignment = new SequenceAlignment(new AlignedBlock(query2, ref, 0, 3, query2RCText.length(), query2RCText.length()), new AlignmentParameters(), false);

    List<SequenceAlignment> components = new ArrayList<SequenceAlignment>();
    components.add(sequence1Alignment);
    components.add(sequence2Alignment);
    QueryAlignment alignment = new QueryAlignment(components, 0, 0, 0, 0, -5);

    MatchDatabase database = new MatchDatabase(0);
    List<QueryAlignment> alignmentList = new ArrayList<QueryAlignment>();
    alignmentList.add(alignment);
    List<List<QueryAlignment>> alignmentListList = new ArrayList<List<QueryAlignment>>();
    alignmentListList.add(alignmentList);
    database.addAlignments(alignmentListList);
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
