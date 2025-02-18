package mapper;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class AncestryDetector_Test {
  @Test
  public void basicTest() {
    String ref1   = "GCCCATTAAAACTGACACGGGTTAC";
    String ref2   = "GCCCATTAAAACTGACACCGGTTAC";
    String union  = "GCCCATTAAAACTGACACSGGTTAC";
    String reference = ref1 + ref1 + ref2;
    String answer    = ref1 + ref1 + union;
    check(reference, answer);
  }

  @Test
  public void test2() {
    String ref1   = "AACGGTGGGAACGGCGGAGCGTCGC";
    String ref2   = "AACGGTGGGATCGGCGGAGCGTCGC";
    String union  = "AACGGTGGGAWCGGCGGAGCGTCGC";
    String reference = ref1 + ref1 + ref2;
    String answer    = ref1 + ref1 + union;
    check(reference, answer);
  }

  @Test
  public void reverseComplementTest() {
    String ref1  = "TTATTGTTAAACCGGTACACC";
    String ref2  = new SequenceBuilder().add(ref1).build().reverseComplement().getText();
    String ref3  = "TTATTGTTAAACCTGTACACC";
    String union = "TTATTGTTAAACCKGTACACC";
    String reference = ref1 + ref2 + ref3;
    String answer    = ref1 + ref2 + union;
    check(reference, answer);
  }

  @Test
  public void proceedPastTiesTest() {
    //                              Tie  Non-tie mismatch
    //                                |  |
    String ref1  = "CAACCGGAGAATCTCGATGAGNNNNNNNN";
    String ref2  = "CAACCGGAGAATCTCGATTAGNNNNNNNN";
    String ref3  = "CAACCGGAGAATCTCGATGAGNNNNNNNN";
    String ref4  = "CAACCGGAGAATCTCGATTATNNNNNNNN";

    String union = "CAACCGGAGAATCTCGATTAKNNNNNNNN";
    String reference = ref1 + ref2 + ref3 + ref4;
    String answer = ref1 + ref2 + ref3 + union;
    check(reference, answer);
  }

  @Test
  public void noChangesTest() {
    String ref1   = "GGACGTACGCACGAACGACCGAGCGATGTTT";
    String ref2   = ref1;
    String union  = ref2;
    String reference = ref1 + ref1 + ref2;
    String answer    = ref1 + ref1 + union;
    check(reference, answer);
  }

  @Test
  public void manyMutationsTest() {
    String ref1   = "AACGACGTCTGACGAGTGACGTGGACAACCGGACGGCTC";
    // Differences:        !      !      !      !      !
    String ref2   = "AACGACTTCTGACAAGTGACCTGGACATCCGGACAGCTC";
    String union  = "AACGACKTCTGACRAGTGACSTGGACAWCCGGACRGCTC";
    String reference = ref1 + ref1 + ref2;
    String answer    = ref1 + ref1 + union;
    check(reference, answer); 
  }

  @Test
  public void breakSimilarSectionTest() {
    String ref1   = "AGCGGTGGAACGGCGGAGCGTCGTCAAACCCGGGTTCTCAGTCG";
    String ref2   = "AGCGGTGGAACGGCGGAGCGTCGTCAAACCCGGGTTCTCAGTCA";
    String suffix = "AGACATACAGAAAGAG";
    String reference_mutatedAtEnd = ref1 + ref1 + ref1 + ref2 + suffix;
    String answer1 = reference_mutatedAtEnd;
    // If the end of the contig appears to be substantially different from the similar section, it's more likely that the similar section has ended than that this new section shares a common history with the similar section and also contains lots of mutations
    check(reference_mutatedAtEnd, answer1);

    String union  = "AGCGGTGGAACGGCGGAGCGTCGTCAAACCCGGGTTCTCAGTCR";
    String reference_mutatedInMiddle = ref1 + ref1 + ref2 + ref1 + suffix;
    String answer2                   = ref1 + ref1 + union + ref1 + suffix;
    // If a small number of differences appear next to a similar section, it's likely that they share common ancestry with the other copies of the similar section
    check(reference_mutatedInMiddle, answer2);
  }

  private void check(String referenceText, String expectedInferredAncestor) {
    Sequence reference = new SequenceBuilder().setName("ref").add(referenceText).build();
    List<Sequence> referenceSequences = new ArrayList<Sequence>();
    referenceSequences.add(reference);
    referenceSequences.add(reference.reverseComplement());
    SequenceDatabase referenceDatabase = new SequenceDatabase(referenceSequences);
    Logger logger = new Logger(new StderrWriter());
    StatusLogger statusLogger = new StatusLogger(logger, 0);
    HashBlock_Database hashblockDatabase = new HashBlock_Database(referenceDatabase);

    int minDuplicationLength = DuplicationDetector.chooseMinDuplicationLength(referenceDatabase);
    int maxDuplicationLength = DuplicationDetector.chooseMaxDuplicationLength(referenceDatabase);
    DuplicationDetector duplicationDetector = new DuplicationDetector(hashblockDatabase, minDuplicationLength, maxDuplicationLength, 3, 0, null, statusLogger);

    double dissimilarityThreshold = 0.3;
    referenceSequences.add(reference);
    AncestryDetector ancestryDetector = new AncestryDetector(duplicationDetector, referenceSequences, dissimilarityThreshold, statusLogger);
    ancestryDetector.setVerifyNoDuplicateAnalyses();

    SequenceDatabase inferredAncestors = ancestryDetector.unionRecentAncestors(logger).getSequenceDatabase();
    List<Sequence> inferredAncestorList = new ArrayList<Sequence>();
    for (int i = 0; i < inferredAncestors.getNumSequences(); i++) {
      Sequence inferred = inferredAncestors.getSequence(i);
      if (inferred.getComplementedFrom() == null)
        inferredAncestorList.add(inferred);
    }
    if (inferredAncestorList.size() != 1) {
      fail("Expected 1 inferred ancestor sequence, got " + inferredAncestorList.size());
    }
    String inferredAncestor = inferredAncestorList.get(0).getText();
    if (!(expectedInferredAncestor.equals(inferredAncestor))) {
      Assert.fail("\nOriginal ref   : '" + referenceText + "'\nExpected result: '" + expectedInferredAncestor + "'\nComputed result: '" + inferredAncestor + "'");
    }
  }

  private void fail(String message) {
    Assert.fail(message);
  }
}
