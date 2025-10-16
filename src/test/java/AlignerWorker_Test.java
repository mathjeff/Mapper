package mapper;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class AlignerWorker_Test {
  @Test
  public void testIndelNotDuplicated() {
    String reference = "TTAAACAGATCACCTCGCTGAGCGGGT";
    String query     = "TTAAACAGATCACCCGCTGAGCGGGT";
    List<QueryAlignment> alignments = align(query, reference);
    verifyOneAlignment(alignments);
  }

  @Test
  public void testPartialAmbiguity() {
    // Mutations:           ;              ;              ;;
    String reference = "AACAGGCGGT" + "AACARGCGGT" + "AACARRCGGT";
    String query =                    "AACAAGCGGT";
    // Aligning to the first reference position should introduce one mutation: G->A
    // Aligning to the second reference position should introduce an ambiguity penalty: R->A
    // Aligning to the third reference position should introduce two ambiguity penalties: R->A and R->G
    // The best alignment should be to the second position
    List<QueryAlignment> alignments = align(query, reference);
    verifyOneAlignment(alignments);
    SequenceAlignment sequenceAlignment = alignments.get(0).getComponent(0);
    verifyAlignment(sequenceAlignment, "AACARGCGGT");
  }

  // tests Illumina-style paired-end-queries where a query can consist of multiple sequences that are expected to align near each other
  @Test
  public void testPairedEndQueries() {
    doTestPairedEndQueries(true, 1);
    doTestPairedEndQueries(false, 0);
  }

  @Test
  public void testHashblockAlsoMatchingNearEndOfContig() {
    String identicalSection = "GGGGTCAC";
    String query = identicalSection + "AAAA";
    String reference = identicalSection + "CAAA" + "TCTCGGAGAGCTCGA" + query + "T";
    List<QueryAlignment> alignments = align(query, reference);
    verifyOneAlignment(alignments);
    SequenceAlignment sequenceAlignment = alignments.get(0).getComponent(0);
    verifyAlignment(sequenceAlignment, query);
  }

  @Test
  public void testFirstHashblockMultipleGoodMatches() {
    String query =           "AACGATCGGG";
    String referenceMatch1 = "AACGATTTGG";
    String referenceMatch2 = "AACGATCGCG";
    String reference = referenceMatch1 + referenceMatch2 + "G";
    List<QueryAlignment> alignments = align(query, reference);
    verifyOneAlignment(alignments);
    SequenceAlignment sequenceAlignment = alignments.get(0).getComponent(0);
    verifyAlignment(sequenceAlignment, referenceMatch2);
  }

  @Test
  public void testOverlappingPairedEndQueriesFewerMutationsOverlappingBothQueries() {
    String query1Prefix =        "AACGAGTG";
    String query1Mutated =       "AAGGACAG";
    String queryOverlap =        "AACGACGGTT";
    String queryOverlapMutated = "AACGAGCGTT";
    String query2Suffix =        "AAAGACCC";

    // 3 mutations in query1 or 2 mutations in each of query1 and query2
    String candidateMatch1 = query1Mutated + queryOverlap + query2Suffix;
    String candidateMatch2 = query1Prefix + queryOverlapMutated + query2Suffix;
    String reference = candidateMatch1 + candidateMatch2;

    Sequence query1 = new SequenceBuilder().setName("query1").add(query1Prefix + queryOverlap).build();
    String query2Text = queryOverlap + query2Suffix;
    query2Text = new SequenceBuilder().setName("temp").add(query2Text).build().reverseComplement().getText();
    Sequence query2 = new SequenceBuilder().setName("query2").add(query2Text).build();

    List<Sequence> queries = new ArrayList<Sequence>();
    queries.add(query1);
    queries.add(query2);
    Query query = new Query(queries, 0, 1000000);
    List<QueryAlignment> alignments = align(query, reference);
    if (alignments.size() != 1) {
      fail("Expected 1 alignment, got " + alignments.size());
    }
    QueryAlignment alignment = alignments.get(0);
    SequenceAlignment query1Alignment = alignment.getComponent(0);
    String alignedTextB = query1Alignment.getAlignedTextB();
    String expectedAlignmentText = query1Prefix + queryOverlapMutated;
    if (!expectedAlignmentText.equals(alignedTextB)) {
      String expectedAlignmentText2 = queryOverlapMutated + query2Suffix;
      String alignedText2 = alignment.getComponent(1).getAlignedTextB();
      fail("Expected:\n  " + query.format() + "\n to align to\n  " + expectedAlignmentText + " / " + expectedAlignmentText2 + "\n not to\n  " + alignedTextB + " / " + alignedText2);
    }
  }

  @Test
  public void testOverlappingPairedEndQueriesBetterThanSurprisingOffset() {
    String query1Text = "ACGTGAACCGGTTAAACCC";
    String query2Text = new SequenceBuilder().setName("temp").add(query1Text).build().reverseComplement().getText();
    Sequence query1 = new SequenceBuilder().setName("query1").add(query1Text).build();
    Sequence query2 = new SequenceBuilder().setName("query2").add(query2Text).build();
    List<Sequence> queries = new ArrayList<Sequence>();
    queries.add(query1);
    queries.add(query2);

    // The mates can completely overlap or can be further away than expected
    String separator = "ACAGTTGGCGAGCGC";
    String referenceText = query1Text + separator + query1Text + "C";

    Query query = new Query(queries, 0, separator.length() / 2);

    List<QueryAlignment> alignments = align(query, referenceText);
    if (alignments.size() != 2) {
      String message = "Expected " + query.format() + " to align to 2 position2 in " + referenceText + ", not " + alignments.size() + "\n\n";
      message += "Reported alignments:\n";
      for (QueryAlignment queryAlignment: alignments) {
        message += queryAlignment.format();
      }
      fail(message);
    }
    QueryAlignment alignment = alignments.get(0);
    SequenceAlignment alignment1 = alignment.getComponent(0);
    SequenceAlignment alignment2 = alignment.getComponent(1);
    if (alignment1.getStartIndexB() != 0) {
      fail("Expected " + query.format() + " first mate first alignment to align to position 0 in " + referenceText + ", not " + alignment1.getStartIndexB());
    }
    if (alignment2.getStartIndexB() != 0) {
      fail("Expected " + query.format() + " second mate first alignment to align to position 0 in " + referenceText + ", not " + alignment2.getStartIndexB());
    }

    QueryAlignment alignmentB = alignments.get(1);
    SequenceAlignment alignmentB1 = alignmentB.getComponent(0);
    SequenceAlignment alignmentB2 = alignmentB.getComponent(1);
    if (alignmentB1.getStartIndexB() != 34) {
      fail("Expected " + query.format() + " first mate second alignment to align to position 0 in " + referenceText + ", not " + alignmentB1.getStartIndexB());
    }
    if (alignmentB2.getStartIndexB() != 34) {
      fail("Expected " + query.format() + " second mate second alignment to align to position 0 in " + referenceText + ", not " + alignmentB2.getStartIndexB());
    }
  }

  @Test
  public void testOverlappingPairedEndQueriesMultipleMatches() {
    String prefix        = "ACGTACGTCC";
    String shared        = "AACCGGTTGG";
    String sharedMutated = "AACCTGTTGG";
    String suffix        = "AAACCCGGGTTT";
    Sequence query1 = new SequenceBuilder().setName("query1").add(prefix + shared).build();
    Sequence query2Reversed = new SequenceBuilder().setName("temp").add(shared + suffix).build();
    Sequence query2 = new SequenceBuilder().setName("query1").add(query2Reversed.reverseComplement().getText()).build();
    String candidateMatch = prefix + sharedMutated + suffix;
    String referenceText = "GGGG" + candidateMatch + candidateMatch + "TTTT";

    double expectedInnerDistance = 0;
    double deviationPerPenalty = candidateMatch.length();
    Query query = new Query(query1, query2, expectedInnerDistance, deviationPerPenalty);
    List<QueryAlignment> alignments = align(query, referenceText);

    if (alignments.size() != 2) {
      String message = "";
      message += "Expected 2 alignments, not " + alignments.size() + ". ";
      message += "All " + alignments.size() + " alignments:\n";
      for (QueryAlignment alignment: alignments) {
        message += alignment.format();
        message += "\n";
      }
      fail(message);
    }
  }

  @Test
  public void testMultipleCandidateMatches() {
    String prefix = ""; //              !                                  !                                      !
    String shared =                 "AACCGGTTCACTCGGGACACACACC" + "ACGTCGTATTGTGCGCCGTTACAAA" + "GTTTGTTTAGAGCCCCTTTTAGCGA";
    String sharedMutated =          "AACTGGTTCACTCGGGACACACACC" + "ACGTCGTAATGTGCGCCGTTACAAA" + "GTTTGTTTAGAGCCCCTCTTAGCGA";
    String suffix =                 "";
    Sequence query1 = new SequenceBuilder().setName("query1").add(prefix + shared).build();
    Sequence query2Reversed = new SequenceBuilder().setName("temp").add(shared + suffix).build();
    Sequence query2 = new SequenceBuilder().setName("query2").add(query2Reversed.reverseComplement().getText()).build();
    String candidateMatch = prefix + sharedMutated + suffix;
    String referenceText = "GGGG" + candidateMatch + "AAAA" + candidateMatch + "TTTT";

    double expectedInnerDistance = -1 * candidateMatch.length();
    double deviationPerPenalty = candidateMatch.length() / 4;
    Query query = new Query(query1, query2, expectedInnerDistance, deviationPerPenalty);
    List<QueryAlignment> alignments = align(query, referenceText);

    if (alignments.size() != 2) {
      String message = "";
      message += "Expected 2 alignments, not " + alignments.size() + ". ";
      message += "All " + alignments.size() + " alignments:\n";
      for (QueryAlignment alignment: alignments) {
        message += "Penalty " + alignment.getPenalty() + ": " + alignment.format();
        message += "\n";
      }
      fail(message);
    }
  }

  @Test
  public void testMultipleCandidateMatches2() {
    String prefix = "G";
    String prefixMutated = "T";
    String shared = "GACATTGGCAAAGTCAACAAAGCGGAAATCAAGGAAGCCATGGACGGCGTATTGAAGAAGATGCAGGGCTTTGACTTTACCAAATTCAAGGAAGAACTTGGTAAGAGAGGTTTTAAAGTCCGGGAAGCCAGGGCAAGCACCGGGAAACTC";
    Sequence query1 = new SequenceBuilder().setName("query1").add(prefix + shared).build();
    Sequence query2Reversed = new SequenceBuilder().setName("temp").add(shared).build();
    Sequence query2 = new SequenceBuilder().setName("query2").add(query2Reversed.reverseComplement().getText()).build();
    String candidateMatch = prefixMutated + shared;
    String referenceText = "C" + candidateMatch + "" + candidateMatch + "TTTT";

    double expectedInnerDistance = -1 * candidateMatch.length();
    double deviationPerPenalty = candidateMatch.length() / 4 / 6;
    Query query = new Query(query1, query2, expectedInnerDistance, deviationPerPenalty);
    AlignmentParameters parameters = new AlignmentParameters();
    parameters.MutationPenalty = 6;
    parameters.InsertionStart_Penalty = 9;
    parameters.InsertionExtension_Penalty = 5.4;
    parameters.DeletionStart_Penalty = 9;
    parameters.DeletionExtension_Penalty = 4.5;
    parameters.MaxErrorRate = 1.2;
    parameters.AmbiguityPenalty = parameters.MaxErrorRate;
    parameters.UnalignedPenalty = parameters.MaxErrorRate;
    List<QueryAlignment> alignments = align(query, referenceText, parameters);

    if (alignments.size() != 2) {
      String message = "";
      message += "Expected 2 alignments, not " + alignments.size() + ". ";
      message += "All " + alignments.size() + " alignments:\n";
      for (QueryAlignment alignment: alignments) {
        message += alignment.format();
        message += "\n";
      }
      fail(message);
    }
  }

  @Test
  public void testPairedEndQueriesRoundingError() {
    String prefix               = "AAACCCGGGTTTAAAACCCCGGGGTTTTAAAAACCCCCGGGGG";
    String shared               = "GACATTGGCAAAGTCAACAAAGCGGAAATCAAGGAAGCCATGGACGGGGTATTGAAGAAGATGCAGGGCTTTGACTTTACCAAATTCAAGGAAGAACTTGGTAAGAG";
    //                                                                            !
    String sharedMutated        = "GACATTGGCAAAGTCAACAAAGCGGAAATCAAGGAAGCCATGGACGGCGTATTGAAGAAGATGCAGGGCTTTGACTTTACCAAATTCAAGGAAGAACTTGGTAAGAG";
    String suffix = "AGGTTTTAAAGTCCGGGAAGCCAGGGCAAGCACCGGGAAACTC";
    Sequence query1 = new SequenceBuilder().setName("query1").add(prefix + sharedMutated).build();
    Sequence query2Reversed = new SequenceBuilder().setName("temp").add(shared + suffix).build();
    Sequence query2 = new SequenceBuilder().setName("query2").add(query2Reversed.reverseComplement().getText()).build();
    String candidateMatch = prefix + shared + suffix;
    String referenceText = "ACGT" + candidateMatch + candidateMatch + "ACGT";

    double expectedInnerDistance = -1 * candidateMatch.length();
    double deviationPerPenalty = candidateMatch.length() / 4 / 6;
    Query query = new Query(query1, query2, expectedInnerDistance, deviationPerPenalty);
    AlignmentParameters parameters = new AlignmentParameters();
    parameters.MutationPenalty = 6;
    parameters.InsertionStart_Penalty = 9;
    parameters.InsertionExtension_Penalty = 5;
    parameters.DeletionStart_Penalty = 6;
    parameters.DeletionExtension_Penalty = 5;
    parameters.MaxErrorRate = 1;
    parameters.AmbiguityPenalty = parameters.MaxErrorRate;
    parameters.UnalignedPenalty = parameters.MaxErrorRate;
    List<QueryAlignment> alignments = align(query, referenceText, parameters);

    if (alignments.size() != 2) {
      String message = "";
      message += "Expected 2 alignments, not " + alignments.size() + ". ";
      message += "All " + alignments.size() + " alignments:\n";
      for (QueryAlignment alignment: alignments) {
        message += alignment.format();
        message += "\n";
      }
      fail(message);
    }
  }

  @Test
  public void testPairedEndQueriesRoundingError2() {
    String prefix        = "ATCCTTGATTTTCCCTTTAAGGGCGTTTATAATCCACCCTTTCGGATTGTTCTTTTCTCGTGATTTTCCGTTTAGGAGAGCCAGTTCTCCGATAAGGTCGGTTATCTTTTCTTGTGCCGTTATGAATGTCTCTTTGTTCCGGTTTAT";
    String shared        = "CTC";
    String suffix        = "TTCCGATGTGAAGCCGCAGGAATAACGGAGGTACTCGTACACATGGCTGTCTATCTGATATCGTGCTGTAACCTTTGCTTGCAATTCTTTCCCTTCCAGTTCTTCATCTCTGAACTGTGGGTGATAGACCGGGTAGAACCTAAACC";
    //                                                                            !
    String suffixMutated = "TTCCGATGTGAAGCCGCAGGAATAACGGAGGTACTCGTACACATGGCTGTCTATATGATATCGTGCTGTAACCTTTGCTTGCAATTCTTTCCCTTCCAGTTCTTCATCTCTGAACTGTGGGTGATAGACCGGGTAGAACCTAAACC";
    Sequence query1 = new SequenceBuilder().setName("query1").add(prefix + shared).build();
    Sequence query2Reversed = new SequenceBuilder().setName("temp").add(shared + suffixMutated).build();
    Sequence query2 = new SequenceBuilder().setName("query2").add(query2Reversed.reverseComplement().getText()).build();
    String candidateMatch = prefix + shared + suffix;
    String referenceText = "ACGT" + candidateMatch + candidateMatch + "ACGT";

    double expectedInnerDistance = -1 * candidateMatch.length();
    double deviationPerPenalty = candidateMatch.length() / 4 / 6;
    Query query = new Query(query1, query2, expectedInnerDistance, deviationPerPenalty);
    AlignmentParameters parameters = new AlignmentParameters();
    parameters.MutationPenalty = 6;
    parameters.InsertionStart_Penalty = 9;
    parameters.InsertionExtension_Penalty = 5;
    parameters.DeletionStart_Penalty = 6;
    parameters.DeletionExtension_Penalty = 5;
    parameters.MaxErrorRate = 1;
    parameters.AmbiguityPenalty = parameters.MaxErrorRate;
    parameters.UnalignedPenalty = parameters.MaxErrorRate;
    List<QueryAlignment> alignments = align(query, referenceText, parameters);

    if (alignments.size() != 2) {
      String message = "";
      message += "Expected 2 alignments, not " + alignments.size() + ". ";
      message += "All " + alignments.size() + " alignments:\n";
      for (QueryAlignment alignment: alignments) {
        message += alignment.format();
        message += "\n";
      }
      fail(message);
    }
  }

  @Test
  public void testPairedEndQueriesRoundingError3() {
    String prefix        = "GAACTGGAAGGGAAAGAAT";
    String shared        = "TGCAAGCAAAGGTTACAGCACGATATCAGATAGACAGCCATGTGTACGAGTACCTCCGTTATTCCTGCGGCTTCACATCGGAAGAGATAAACCGGAACAAAGAGACATTCATAACGGAACAAGAAAAGATA";
    //                                                                                                                                           !
    String sharedMutated = "TGCAAGCAAAGGTTACAGCACGATATCAGATAGACAGCCATGTGTACGAGTACCTCCGTTATTCCTGCGGCTTCACATCGGAAGAGATAAACCGGAACAAAGAGACATTCATAACGGCACAAGAAAAGATA";
    String suffix        = "ACCGACCTTATCGGAGA";
    Sequence query1 = new SequenceBuilder().setName("query1").add(prefix + sharedMutated).build();
    Sequence query2Reversed = new SequenceBuilder().setName("temp").add(shared + suffix).build();
    Sequence query2 = new SequenceBuilder().setName("query2").add(query2Reversed.reverseComplement().getText()).build();
    String candidateMatch = prefix + shared + suffix;
    String referenceText = "ACGT" + candidateMatch + candidateMatch + "ACGT";

    double expectedInnerDistance = -1 * candidateMatch.length();
    double deviationPerPenalty = candidateMatch.length() / 4 / 6;
    Query query = new Query(query1, query2, expectedInnerDistance, deviationPerPenalty);
    AlignmentParameters parameters = new AlignmentParameters();
    parameters.MutationPenalty = 6;
    parameters.InsertionStart_Penalty = 9;
    parameters.InsertionExtension_Penalty = 5;
    parameters.DeletionStart_Penalty = 6;
    parameters.DeletionExtension_Penalty = 5;
    parameters.MaxErrorRate = 1;
    parameters.AmbiguityPenalty = parameters.MaxErrorRate;
    parameters.UnalignedPenalty = parameters.MaxErrorRate;
    List<QueryAlignment> alignments = align(query, referenceText, parameters);

    if (alignments.size() != 2) {
      String message = "";
      message += "Expected 2 alignments, not " + alignments.size() + ". ";
      message += "All " + alignments.size() + " alignments:\n";
      for (QueryAlignment alignment: alignments) {
        message += alignment.format();
        message += "\n";
      }
      fail(message);
    }
  }

  @Test
  public void testPairedEndQueriesRoundingError4() {
    String prefix        = "GAACAAGGCACATGACGGTCTGGAAAACAATCCGGGAAAAGACGGCAAACT";
    //                                                       !
    String prefixMutated = "GAACAAGGCACATGACGGTCTGGAAAACAATCCAGGAAAAGACGGCAAACT";
    String shared        = "GTTTTCAGACAAACACCCCTACATTACTGAAGCGCATCCGGGAGCAAAAAAAGCCGTGGACGCACTGACCAGGCGCATCAACGAAATGATAGCCGAAAT";
    String suffix        = "GCCGGACAACCTGACGCTGGAGGAAAAAACCGACATCGCCCGCAACAATCT";
    //                       !
    String suffixMutated = "GTCGGACAACCTGACGCTGGAGGAAAAAACCGACATCGCCCGCAACAATCT";
    Sequence query1 = new SequenceBuilder().setName("query1").add(prefixMutated + shared).build();
    Sequence query2Reversed = new SequenceBuilder().setName("temp").add(shared + suffixMutated).build();
    Sequence query2 = new SequenceBuilder().setName("query2").add(query2Reversed.reverseComplement().getText()).build();
    String candidateMatch = prefix + shared + suffix;
    String referenceText = "ACGT" + candidateMatch + candidateMatch + "ACGT";

    double expectedInnerDistance = -1 * candidateMatch.length();
    double deviationPerPenalty = candidateMatch.length() / 4 / 6;
    Query query = new Query(query1, query2, expectedInnerDistance, deviationPerPenalty);
    AlignmentParameters parameters = new AlignmentParameters();
    parameters.MutationPenalty = 6;
    parameters.InsertionStart_Penalty = 9;
    parameters.InsertionExtension_Penalty = 5;
    parameters.DeletionStart_Penalty = 6;
    parameters.DeletionExtension_Penalty = 5;
    parameters.MaxErrorRate = 1;
    parameters.AmbiguityPenalty = parameters.MaxErrorRate;
    parameters.UnalignedPenalty = parameters.MaxErrorRate;
    List<QueryAlignment> alignments = align(query, referenceText, parameters);

    if (alignments.size() != 2) {
      String message = "";
      message += "Expected 2 alignments, not " + alignments.size() + ". ";
      message += "All " + alignments.size() + " alignments:\n";
      for (QueryAlignment alignment: alignments) {
        message += alignment.format();
        message += "\n";
      }
      fail(message);
    }
  }

  @Test
  public void testPairedEndQueriesRoundingError5() {
    String prefix        = "TCTTTGTAGGGTGAAAGAGAAACCCATAAACGGGGATAGATTGAATGCTGGGAAGCATAAACAATC";
    String shared        = "GGGGTAAGGTTAGCGAACCTTGCCTTTCATCCCCCATTATAACTTTACATAGAGGAACTTTATCTATCCCCCCCCGCCCCCAAA";
    //                                     !           !
    String sharedMutated = "GGGGTAAGGTTAGCGTACCTTGCCTTTGATCCCCCATTATAACTTTACATAGAGGAACTTTATCTATCCCCCCCCGCCCCCAAA";
    String suffix        = "GGGGGAGCGACCAAACGGCAGCTTCACTCAATGGAGTGTTACAGTTCATCAAAACCAAGTGATAAC";

    Sequence query1 = new SequenceBuilder().setName("query1").add(prefix + shared).build();
    Sequence query2Reversed = new SequenceBuilder().setName("temp").add(sharedMutated + suffix).build();
    Sequence query2 = new SequenceBuilder().setName("query2").add(query2Reversed.reverseComplement().getText()).build();
    String candidateMatch = prefix + shared + suffix;
    String referenceText = "ACGT" + candidateMatch + candidateMatch + "ACGT";

    double expectedInnerDistance = -1 * candidateMatch.length();
    double deviationPerPenalty = candidateMatch.length() / 4 / 6;
    Query query = new Query(query1, query2, expectedInnerDistance, deviationPerPenalty);
    AlignmentParameters parameters = new AlignmentParameters();
    parameters.MutationPenalty = 6;
    parameters.InsertionStart_Penalty = 9;
    parameters.InsertionExtension_Penalty = 5;
    parameters.DeletionStart_Penalty = 6;
    parameters.DeletionExtension_Penalty = 5;
    parameters.MaxErrorRate = 1;
    parameters.AmbiguityPenalty = parameters.MaxErrorRate;
    parameters.UnalignedPenalty = parameters.MaxErrorRate;
    List<QueryAlignment> alignments = align(query, referenceText, parameters);

    if (alignments.size() != 2) {
      String message = "";
      message += "Expected 2 alignments, not " + alignments.size() + ". ";
      message += "All " + alignments.size() + " alignments:\n";
      for (QueryAlignment alignment: alignments) {
        message += alignment.format();
        message += "\n";
      }
      fail(message);
    }
  }

  @Test
  public void testPairedEndQueriesRoundingError6() {
    String prefix        =  "CAATAGGGAGATAACAGCACAAAGGATTGAGTAGAACGAAATTCGTTTGTCCACATAACCGCCGTTTTTCAT";

    String suffixMutated =  "TGTACCTTTCGGGCTGTTGCGTCCTCTATGCGCTTCGTATAGACTTCAACACGCTTTAGTTCTTGATACACC";
    String suffix        =  "TGTACCTTTCGGGCTGTTGCGTCCTCTATGCGCTTCGTATAGACTTCAACACGCTTTAGTTCTTGATACACC";

    String sharedMutated =  "TCTGTACCCCTGCCGTTCAAAGTCCGCCAACACGTTTTTAGGCGATTTTCGGCACTTTCTAGGCTTTTCCCGTCTATT";
    //                                                        !                         !
    String shared        = "TCTGTACCCCTGCCGTTCAAAGTCCGCCAACACGTTTTTTAGGCGATTTTCGGCACTTTCAAGGCTTTTCCCGTCTATT";


    Sequence query1 = new SequenceBuilder().setName("query1").add(prefix + sharedMutated).build();
    Sequence query2Reversed = new SequenceBuilder().setName("temp").add(sharedMutated + suffixMutated).build();
    Sequence query2 = new SequenceBuilder().setName("query2").add(query2Reversed.reverseComplement().getText()).build();
    String candidateMatch = prefix + shared + suffix;
    String referenceText = "ACGT" + candidateMatch + candidateMatch + "ACGT";

    double expectedInnerDistance = -1 * candidateMatch.length();
    double deviationPerPenalty = candidateMatch.length() / 4 / 6;
    Query query = new Query(query1, query2, expectedInnerDistance, deviationPerPenalty);
    AlignmentParameters parameters = new AlignmentParameters();
    parameters.MutationPenalty = 6;
    parameters.InsertionStart_Penalty = 9;
    parameters.InsertionExtension_Penalty = 5;
    parameters.DeletionStart_Penalty = 6;
    parameters.DeletionExtension_Penalty = 5;
    parameters.MaxErrorRate = 1;
    parameters.AmbiguityPenalty = parameters.MaxErrorRate;
    parameters.UnalignedPenalty = parameters.MaxErrorRate;
    List<QueryAlignment> alignments = align(query, referenceText, parameters);

    if (alignments.size() != 2) {
      String message = "";
      message += "Expected 2 alignments, not " + alignments.size() + ". ";
      message += "All " + alignments.size() + " alignments:\n";
      for (QueryAlignment alignment: alignments) {
        message += alignment.format();
        message += "\n";
      }
      fail(message);
    }
  }

  @Test
  public void testPairedEndQueriesOverlappingIndel() {
    String shared        =    "CTTCCATATCTGTTTGCTTTTAAATTCAGCACAAAGATAGCTATATTTCAATAAAATACAAACATTTTGTACACAAACGTGTACACGCCATAAAAACCCGTTTCCAATCCTACCGCCCGTTGGTTGGTTTTGCTTTGCTCTTTTTCCC";
    String sharedMutated = "ATGCTTCCATATCTGTTTGCTTTTAAATTCAGCACAAAGATAGCTATATTTCAATAAAATACAAACATTTTGTACACAAACGTGTACACGCCATAAAAACCCGTTTCCAATCCTACCGCCCGTTGGTTGGTTTTGCTTTGCTCTTTTTCCCT";
    String suffix        = "CT";
    String prefix        = "AG";
    Sequence query1 = new SequenceBuilder().setName("query1").add(shared + suffix).build();
    Sequence query2Reversed = new SequenceBuilder().setName("temp").add(prefix + shared).build();
    Sequence query2 = new SequenceBuilder().setName("query2").add(query2Reversed.reverseComplement().getText()).build();
    String candidateMatch = sharedMutated;
    String referenceText = "ACGT" + candidateMatch + "AACCGGTT" + candidateMatch + "ACGT";

    double expectedInnerDistance = -1 * candidateMatch.length();
    double deviationPerPenalty = candidateMatch.length() / 4 / 6;
    Query query = new Query(query1, query2, expectedInnerDistance, deviationPerPenalty);
    AlignmentParameters parameters = new AlignmentParameters();
    parameters.MutationPenalty = 6;
    parameters.InsertionStart_Penalty = 3;
    parameters.InsertionExtension_Penalty = 2;
    parameters.DeletionStart_Penalty = 3;
    parameters.DeletionExtension_Penalty = 2;
    parameters.MaxErrorRate = 1;
    parameters.AmbiguityPenalty = parameters.MaxErrorRate;
    parameters.UnalignedPenalty = parameters.MaxErrorRate;
    List<QueryAlignment> alignments = align(query, referenceText, parameters);

    if (alignments.size() != 2) {
      String message = "";
      message += "Expected 2 alignments, not " + alignments.size() + ". ";
      message += "All " + alignments.size() + " alignments:\n";
      for (QueryAlignment alignment: alignments) {
        message += alignment.format();
        message += "\n";
      }
      fail(message);
    }
  }

  @Test
  public void testPairedEndQueriesOverlappingInsertion() {
    String prefix = "TCTCGGCTGGCGGCAAGAGAAGAGAACACCTCGTGCAT";
    String shared        = "AGGCTCGCCGTTCTCTAACCAGTAAACACAATATTCGACCATAACAGTTTTATCATTTATCGTTGTAATGCCCCTCTACCTCCAAGATGTAGACCTCTACCACTTCCTCGTA";
    //                                                                                            !
    String sharedMutated = "AGGCTCGCCGTTCTCTAACCAGTAAACACAATATTCGACCATAACAGTTTTATCATTTATCGTTGTAATGCCCCCTCTACCTCCAAGATGTAGACCTCTACCACTTCCTCGTA";
    String suffix = "AATGTCATAGATTATCCGGTCATGGGCGGTAATGTGT";


    Sequence query1 = new SequenceBuilder().setName("query1").add(prefix + sharedMutated).build();
    Sequence query2Reversed = new SequenceBuilder().setName("temp").add(sharedMutated + suffix).build();
    Sequence query2 = new SequenceBuilder().setName("query2").add(query2Reversed.reverseComplement().getText()).build();
    String candidateMatch = prefix + shared + suffix;
    String referenceText = "ACGT" + candidateMatch + "ACGT" + candidateMatch + "ACGT";

    double expectedInnerDistance = -1 * shared.length();
    double deviationPerPenalty = 0.5;
    Query query = new Query(query1, query2, expectedInnerDistance, deviationPerPenalty);
    AlignmentParameters parameters = new AlignmentParameters();
    parameters.MutationPenalty = 1;
    parameters.InsertionStart_Penalty = 1.5;
    parameters.InsertionExtension_Penalty = 0.6;
    parameters.DeletionStart_Penalty = 1.5;
    parameters.DeletionExtension_Penalty = 0.5;
    parameters.MaxErrorRate = 0.05;
    parameters.AmbiguityPenalty = parameters.MaxErrorRate;
    parameters.UnalignedPenalty = parameters.MaxErrorRate;
    List<QueryAlignment> alignments = align(query, referenceText, parameters);

    if (alignments.size() != 2) {
      String message = "";
      message += "Expected 2 alignments, not " + alignments.size() + ". ";
      message += "All " + alignments.size() + " alignments:\n";
      for (QueryAlignment alignment: alignments) {
        message += alignment.format();
        message += "\n";
      }
      fail(message);
    }
  }

  @Test
  public void testPairedEndQueriesWithIndelsNextToOverlap() {
    String prefix = "AACCGGTT";
    String prefixMutated = "AACCGG";
    String shared = "GACATTGGCAAAGTCAACAAAGCGGAAATCAAGGAAGCCATGGACGGCGTATTGAAGAAGATGCAGGGCTTTGACTTTACCAAATTCAAGGAAGAACTTGGTAAGAGAGGTTTTAAAGTCCGGGAAGCCAGGGCAAGCACCGGGAAACTC";
    String suffix = "AACCGGTT";
    String suffixMutated = "CCGGTT";
    Sequence query1 = new SequenceBuilder().setName("query1").add(prefix + shared).build();
    Sequence query2Reversed = new SequenceBuilder().setName("temp").add(shared + suffix).build();
    Sequence query2 = new SequenceBuilder().setName("query2").add(query2Reversed.reverseComplement().getText()).build();
    String candidateMatch = prefixMutated + shared + suffixMutated;
    String referenceText = "ACGT" + candidateMatch + "ACGT" + candidateMatch + "ACGT";

    double expectedInnerDistance = -1 * candidateMatch.length();
    double deviationPerPenalty = 1;
    Query query = new Query(query1, query2, expectedInnerDistance, deviationPerPenalty);
    AlignmentParameters parameters = new AlignmentParameters();
    parameters.MutationPenalty = 1;
    parameters.InsertionStart_Penalty = 1.5;
    parameters.InsertionExtension_Penalty = 0.6;
    parameters.DeletionStart_Penalty = 1.5;
    parameters.DeletionExtension_Penalty = 0.5;
    parameters.MaxErrorRate = 0.05;
    parameters.AmbiguityPenalty = parameters.MaxErrorRate;
    parameters.UnalignedPenalty = parameters.MaxErrorRate;
    List<QueryAlignment> alignments = align(query, referenceText, parameters);

    if (alignments.size() != 2) {
      String message = "";
      message += "Expected 2 alignments, not " + alignments.size() + ". ";
      message += "All " + alignments.size() + " alignments:\n";
      for (QueryAlignment alignment: alignments) {
        message += alignment.format();
        message += "\n";
      }
      fail(message);
    }
  }

  @Test
  public void testDeletionInMiddleOfQueryWithMultipleAlignments() {
    String prefix = "ACCGTAACAACCTCGCAGCGTCTTTCACCAAAGCTGACAATGGCGAGCAGGTACTAATTCGCA";
    String deletion = "G";
    String suffix = "GAAAAACGAGATTTACGCTTTGGTAAAAGTTGGTCGTGAAGATTTGATGATAACCCCGGAGCTGCAAGCAAGGATTGACAAGGCAAG";
    Sequence querySequence = new SequenceBuilder().setName("query").add(prefix + suffix).build();
    String match = prefix + deletion + suffix;
    String referenceText = "A" + match + match + "A";

    Query query = new Query(querySequence);
    AlignmentParameters parameters = makeParameters();
    List<QueryAlignment> alignments = align(query, referenceText, parameters);

    if (alignments.size() != 2) {
      String message = "";
      message += "Expected 2 alignments, not " + alignments.size() + ". ";
      message += "All " + alignments.size() + " alignments:\n";
      for (QueryAlignment alignment: alignments) {
        message += alignment.format();
        message += "\n";
      }
      fail(message);
    }
  }

  @Test
  public void queryExtendingPastEndOfReference() {
    String queryText =                                                                                  "ATCCTACAGCAACTCAATTGAGTTTAGGTGTGACTCTTCGCTTCAAATAAATGAGAAACAAATTATTAAAAATATGAAAGATATGAAATATATAAAATGTC";
    String referenceText = "GACCGGATATTCTGGTAATGACCCTTCAATTATAGACGTGAATGGTATCCAGCCGGGAGTAGATAGTAATAGTGCTTATCCTACAGCAACTCAATTGAGTTTAGGTGTGAC";
    Sequence querySequence = new SequenceBuilder().setName("query").add(queryText).build();
    Query query = new Query(querySequence);

    AlignmentParameters parameters = makeParameters();
    List<QueryAlignment> alignments = align(query, referenceText, parameters);
    verifyOneAlignment(alignments);
    QueryAlignment alignment = alignments.get(0);
    String alignedB = alignment.getComponent(0).getAlignedTextB();
    String expectedB = "ATCCTACAGCAACTCAATTGAGTTTAGGTGTGAC";
    if (!alignedB.equals(expectedB)) {
      fail("expected aligned text b of " + expectedB + " not " + alignedB);
    }
  }

  @Test
  public void testCustomParameters() {
    String queryText =     "ACGCACCTCTTTT";
    String referenceText =  "CGCGACTCT";

    Sequence querySequence = new SequenceBuilder().setName("query").add(queryText).build();
    Query query = new Query(querySequence);

    AlignmentParameters parameters = new AlignmentParameters();
    parameters.MutationPenalty = 1;
    parameters.InsertionStart_Penalty = 0.8;
    parameters.InsertionExtension_Penalty = 1;
    parameters.DeletionStart_Penalty = 0.8;
    parameters.DeletionExtension_Penalty = 1;
    parameters.MaxErrorRate = 0.7;
    parameters.AmbiguityPenalty = 0.9;
    parameters.UnalignedPenalty = 0.9;

    List<QueryAlignment> alignments = align(query, referenceText, parameters);
    verifyOneAlignment(alignments);

    QueryAlignment alignment = alignments.get(0);
    String alignedB = alignment.getComponent(0).getAlignedTextB();
    String expectedA = "CGCACCTCT";
    String expectedB = "CGCGACTCT";
    if (!alignedB.equals(expectedB)) {
      fail("expected alignment of:\n" + expectedA + "\n" + expectedB + ", not\n" + alignment.format());
    }
  }

  @Test
  public void testLongCheapIndel() {
    String referencePrefix = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    String queryPrefix        = "AACACACGGTGTTCAC";
    //                              !
    String queryPrefixMutated = "AACCCACGGTGTTCAC";
    String insertion = "CACCCGCCCGCGCGCTCTCTCG";
    String sharedSuffix = "AATAACCGCCGGCGGTTATTAAAACCCCGGGGTTTTAAACCCGGGTTTAACCGGTTACGT";
    String referenceSuffix = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    String queryText =                       queryPrefix        + insertion + sharedSuffix;
    String referenceText = referencePrefix + queryPrefixMutated +             sharedSuffix + queryPrefix + referenceSuffix;

    AlignmentParameters parameters = makeParameters();
    parameters.InsertionExtension_Penalty = 0.2;
    parameters.DeletionExtension_Penalty = 0.2;
    parameters.MutationPenalty = 2;
    Query query = new Query(new SequenceBuilder().setName("query").add(queryText).build());

    List<QueryAlignment> alignments = align(query, referenceText, parameters);
    QueryAlignment alignment = verifyOneAlignment(alignments);
    verifyAlignment(alignment.getComponent(0), queryPrefixMutated + repeat("-",  insertion.length()) + sharedSuffix);
  }

  private void doTestPairedEndQueries(boolean reverseQuerySequence2, int expectedNumMatches) {
    String reference = "AAAAAAAAAAACGGAAAGAAATAACTTAAACGAACTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACGGAAAGAAATAAA";
    String sequence1 =            "CGGAAAGAAA";                                                        // could also be here
    String sequence2 =                         "CTTAAACGAACT";
    if (reverseQuerySequence2) {
      sequence2 = new SequenceBuilder().setName("temp").add(sequence2).build().reverseComplement().getText();
    }
    Sequence query1 = new SequenceBuilder().setName("q1").add(sequence1).build();
    Sequence query2 = new SequenceBuilder().setName("q2").add(sequence2).build();

    List<QueryAlignment> query1Alignments = align(query1, reference);
    if (query1Alignments.size() != 2) {
      fail("Expected 2 alignments for query1, got " + query1Alignments.size());
    }

    List<QueryAlignment> query2Alignments = align(query2, reference);
    if (query2Alignments.size() != 1) {
      fail("Expected 1 alignment for query2, got " + query2Alignments.size());
    }

    List<Sequence> querySequences = new ArrayList<Sequence>();
    querySequences.add(query1);
    querySequences.add(query2);
    int expectedInnerDistance = 3;
    double deviationPerPenalty = 1;
    Query combinedQuery = new Query(querySequences, expectedInnerDistance, deviationPerPenalty);
    List<QueryAlignment> combinedAlignments = align(combinedQuery, reference);
    if (combinedAlignments.size() != expectedNumMatches) {
      fail("With query2.reversed = " + reverseQuerySequence2 + ", expected " + expectedNumMatches + " combined alignments, got " + combinedAlignments.size());
    }
  }

  private List<QueryAlignment> align(String queryText, String referenceText) {
    Sequence querySequence = new SequenceBuilder().setName("query").add(queryText).build();
    return align(querySequence, referenceText);
  }

  private List<QueryAlignment> align(Sequence querySequence, String referenceText) {
    return align(new Query(querySequence), referenceText);
  }

  private List<QueryAlignment> align(Query query, String referenceText) {
    return align(query, referenceText, makeParameters());
  }

  private List<QueryAlignment> align(Query query, String referenceText, AlignmentParameters parameters) {
    if (referenceText.length() <= 1000)
      System.err.println("Aligning to " + referenceText);
    return Api.alignOnce(query, referenceText, parameters, new Logger(new StderrWriter()));
  }

  private QueryAlignment verifyOneAlignment(List<QueryAlignment> alignments) {
    if (alignments.size() != 1) {
      StringBuilder builder = new StringBuilder();
      builder.append("Expected 1 alignment, got " + alignments.size() + ":\n");
      for (QueryAlignment queryAlignment : alignments) {
        for (SequenceAlignment alignment : queryAlignment.getComponents()) {
          builder.append("\n" + alignment.getAlignedTextA() + "\n");
          builder.append(alignment.getAlignedTextB() + "\n");
        }
      }
      fail(builder.toString());
    }
    return alignments.get(0);
  }

  private void verifyAlignment(SequenceAlignment alignment, String expectedReference) {
    if (!alignment.getAlignedTextB().equals(expectedReference)) {
      fail("Incorrect alignment:\n" + alignment.format() + "\n  Expected reference:\n" + expectedReference);
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
    parameters.MaxErrorRate = 0.2;
    parameters.AmbiguityPenalty = parameters.MaxErrorRate;
    parameters.UnalignedPenalty = parameters.MaxErrorRate;
    return parameters;
  }

  private String repeat(String component, int count) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < count; i++) {
      builder.append(component);
    }
    return builder.toString();
  }
}
