package mapper;

import org.junit.Assert;
import org.junit.Test;

public class HashBlockAligner_Test {
  public HashBlockAligner_Test() {
  }

  @Test
  public void testQueryWithLongInsertion() {
    String query        = "GAGTGTCAATGACTGTTCGGCAACGGACATACTCCCGAACAGTCATTGACACTCCGTCCCACTCACGGAGAAGAGATTCTGCTGCAACCGGGCATCAACT";
    String ref          = "AAAAAAAAACAGCGCAAAGAGCTGTTCGGCAACGGACATACTCCCGAATAGTCCTTGACACTCCGTCCCACTCACGGAGAAGAGATGCTGCTGCAACCGGGCATCAACTAAAAAAAAA";
    String alignedQuery =  query;
    String alignedRef   = "GAG---------CTGTTCGGCAACGGACATACTCCCGAATAGTCCTTGACACTCCGTCCCACTCACGGAGAAGAGATGCTGCTGCAACCGGGCATCAACT";
    check(query, ref, alignedQuery, alignedRef, 9.9);
  }

  @Test
  public void testInsertionCoveringThreeHashblocks() {
    String query        = "CACGCACAATGGCATGACAGCCAACAACAAAAGTAAAAAAATCGATTTTGTTCGCATGGTAGTATTAATAGGTTTATTGATGAAGCAAAGTGTGTCTCTTAAAGAAAT";
    String ref          = "AAAAAAAAACACGCACAATGGCATGACAGCCAACAACAAAAGTAAAAAAATCGATTTTGTTCGCATGGTAGTATTAATAGGTTTATTGATGAAGCAAAGTAAAGAAATAAATCACTTTCCCGCCAAATTTAAAAAAAAA";
    String alignedQuery = "CACGCACAATGGCATGACAGCCAACAACAAAAGTAAAAAAATCGATTTTGTTCGCATGGTAGTATTAATAGGTTTATTGATGAAGCAAAGTGTGTCTCTTAAAGAAAT";
    String alignedRef   = "CACGCACAATGGCATGACAGCCAACAACAAAAGTAAAAAAATCGATTTTGTTCGCATGGTAGTATTAATAGGTTTATTGATGAAGCAAAG---------TAAAGAAAT";
    check(query, ref, alignedQuery, alignedRef, 6.9);
  }

  @Test
  public void testQueryExtendingPastEndOfReference() {
    String query = "TTTGATTCCTGTCTGATTCCCGTTCAATTCCCGCCAAGGTCCCACCGAGTTTTTTGCTTAAACCCCGTTTAATTTGCGTCAAGTTCCCGTTAAACTCCCT";
    String ref   = "TTTGATTCCTGTCTGATTCCCG";
    String alignedQuery = ref;
    String alignedRef   = ref;
    AlignmentParameters parameters = makeParameters();
    parameters.MaxErrorRate = 0.09;
    check(query, ref, alignedQuery, alignedRef, 7.8, parameters);
  }

  @Test
  public void testQueryAlignedToMiddleOfReference() {
    String query =                                  "AACGT";
    String ref   = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACGTAAAAAAAAAAAAAA";
    String alignedQuery = query;
    String alignedRef   = query;
    AlignmentParameters parameters = makeParameters();
    parameters.MaxErrorRate = 0.5;
    check(query, ref, alignedQuery, alignedRef, 0, parameters);
  }

  private void check(String textA, String textB, String alignedTextA, String alignedTextB, double expectedPenalty) {
    check(textA, textB, alignedTextA, alignedTextB, expectedPenalty, makeParameters());
  }

  private void check(String textA, String textB, String alignedTextA, String alignedTextB, double expectedPenalty, AlignmentParameters parameters) {
    Sequence a = new SequenceBuilder().setName("a").add(textA).build();
    Sequence b = new SequenceBuilder().setName("b").add(textB).build();
    SequenceMatch match = new SequenceMatch(a, b, 0);
    HashBlock_Aligner aligner = new HashBlock_Aligner(new StraightAligner(new PathAligner_Runner()));
    aligner.setLogger(new Logger(new PrintWriter()));
    AlignmentAnalysis analysis = new AlignmentAnalysis();
    analysis.maxInsertionExtensionPenalty = expectedPenalty;
    analysis.maxDeletionExtensionPenalty = expectedPenalty;
    SequenceAlignment result = aligner.align(new SequenceSection(a, 0, a.getLength()), new SequenceSection(b, 0, b.getLength()), parameters, analysis);
    System.err.println("penalty = " + result.getPenalty());
    check(result, alignedTextA, alignedTextB, expectedPenalty);
  }

  private void check(SequenceAlignment alignment, String alignedTextA, String alignedTextB, double expectedPenalty) {
    System.err.println("alignment penalty = " + alignment.getPenalty());
    if (!alignedTextA.equals(alignment.getAlignedTextA())) {
      fail("Expected alignment text a of " + alignedTextA + " with penalty " + expectedPenalty + ", got alignment of \n" + alignment.format() + " with penalty " + alignment.getPenalty());
    }
    if (!alignedTextB.equals(alignment.getAlignedTextB())) {
      fail("Expected alignment text b of " + alignedTextB + " with penalty " + expectedPenalty + ", got alignment of \n" + alignment.format() + " with penalty " + alignment.getPenalty());
    }
    if (Math.abs(alignment.getPenalty() - expectedPenalty) > 0.000001) {
      fail("Expected alignment penalty " + expectedPenalty + ", got alignment penalty " + alignment.getPenalty());
    }
  }

  private void fail(String message) {
    Assert.fail(message);
  }
  private AlignmentParameters makeParameters() {
    AlignmentParameters parameters = new AlignmentParameters();
    parameters.MutationPenalty = 1;
    parameters.InsertionStart_Penalty = 1.5;
    parameters.InsertionExtension_Penalty = 0.6;
    parameters.DeletionStart_Penalty = 1.5;
    parameters.DeletionExtension_Penalty = 0.5;
    parameters.MaxErrorRate = 0.1;
    parameters.MaxNumMatches = 1;
    parameters.AmbiguityPenalty = 0.1;
    return parameters;
  }
}
