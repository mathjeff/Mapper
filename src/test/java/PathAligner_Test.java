package mapper;

import org.junit.Assert;
import org.junit.Test;

public class PathAligner_Test {
  public PathAligner_Test() {
  }

  @Test
  public void testQueryEndingWithMismatchAndExtension() {
    check("AACCGGTT", "AAT", "AAC", "AAT", 1.5);
  }

  @Test
  public void testQueryStartingWithShortExtension() {
    String query        =   "AAACCGGTTACGTACGTACGT";
    String ref          =   "AACCGGTTACGTTACGTACGT";
    String alignedQuery =   "AACCGGTTACG-TACGTACGT";
    String alignedRef   =   "AACCGGTTACGTTACGTACGT";
    check(query, ref, alignedQuery, alignedRef, 3.1);
  }

  private void check(String textA, String textB, String alignedTextA, String alignedTextB, double expectedPenalty) {
    Sequence a = new SequenceBuilder().setName("a").add(textA).build();
    Sequence b = new SequenceBuilder().setName("b").add(textB).build();
    SequenceMatch match = new SequenceMatch(a, b, 0);

    PathAligner aligner = new PathAligner(new Logger(new StderrWriter()));
    AlignmentAnalysis analysis = new AlignmentAnalysis();
    analysis.maxInsertionExtensionPenalty = expectedPenalty;
    analysis.maxDeletionExtensionPenalty = expectedPenalty;
    SequenceAlignment result = aligner.align(new SequenceSection(a, 0, a.getLength()), new SequenceSection(b, 0, b.getLength()), makeParameters(), analysis);
    check(result, alignedTextA, alignedTextB, expectedPenalty);
  }

  private void check(SequenceAlignment alignment, String alignedTextA, String alignedTextB, double expectedPenalty) {
    if (alignment == null) {
      fail("Expected alignment of " + alignedTextA + " / " + alignedTextB + ", not null");
    }
    if (alignment.getPenalty() != expectedPenalty) {
      fail("Expected alignment penalty of " + expectedPenalty + " for alignment\n" + alignedTextA + "\n" + alignedTextB + "\nbut got " + alignment.getPenalty() + " for alignment:\n" + alignment.format());
    }
    if (!alignedTextA.equals(alignment.getAlignedTextA())) {
      fail("Expected alignment text a of " + alignedTextA + "\nbut got alignment (with same penalty) of \n" + alignment.format());
    }
    if (!alignedTextB.equals(alignment.getAlignedTextB())) {
      fail("Expected alignment text b of " + alignedTextB + "\nbut got alignment (with same penalty) of \n" + alignment.format());
    }
  }

  private void fail(String message) {
    Assert.fail(message);
  }
  private AlignmentParameters makeParameters() {
    AlignmentParameters parameters = new AlignmentParameters();
    parameters.MutationPenalty = 1;
    parameters.InsertionStart_Penalty = 2;
    parameters.InsertionExtension_Penalty = 0.5;
    parameters.DeletionStart_Penalty = 2;
    parameters.DeletionExtension_Penalty = 1;
    parameters.MaxErrorRate = 1;
    parameters.AmbiguityPenalty = 0.1;
    parameters.UnalignedPenalty = parameters.AmbiguityPenalty;
    return parameters;
  }
}
