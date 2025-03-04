package mapper;

import org.junit.Assert;
import org.junit.Test;

public class BasepairsTest {

  @Test
  public void penaltyTest() {
    byte A = Basepairs.encode('A');
    byte C = Basepairs.encode('C');
    byte N = Basepairs.encode('N');
    byte AOrC = Basepairs.union(A, C);

    AlignmentParameters alignmentParameters = new AlignmentParameters();
    double ambiguityPenalty = 3;
    alignmentParameters.AmbiguityPenalty = ambiguityPenalty;
    double mutationPenalty = 100;
    alignmentParameters.MutationPenalty = mutationPenalty;

    double AToCPenalty = Basepairs.getPenalty(A, C, alignmentParameters);
    if (AToCPenalty != mutationPenalty) {
      fail("Expected A to C penalty to be " + mutationPenalty + ", not " + AToCPenalty);
    }

    double AToNPenalty = Basepairs.getPenalty(A, N, alignmentParameters);
    if (AToNPenalty != ambiguityPenalty) {
      fail("Expected A to N penalty to be " + ambiguityPenalty + ", not " + ambiguityPenalty);
    }
    double NToAPenalty = Basepairs.getPenalty(N, A, alignmentParameters);
    if (ambiguityPenalty != AToNPenalty) {
      fail("Expected N to A penalty to be " + ambiguityPenalty + ", not " + ambiguityPenalty);
    }

    double expectedPartialAmbiguityPenalty = ambiguityPenalty / 3;
    double AToAOrCPenalty = Basepairs.getPenalty(A, AOrC, alignmentParameters);
    if (AToAOrCPenalty != expectedPartialAmbiguityPenalty) {
      fail("Expected A to (A or C) penalty to be " + expectedPartialAmbiguityPenalty + ", not " + AToAOrCPenalty);
    }

    double AOrCToAPenalty = Basepairs.getPenalty(AOrC, A, alignmentParameters);
    if (AOrCToAPenalty != expectedPartialAmbiguityPenalty) {
      fail("Expected (A or C) to A penalty to be " + expectedPartialAmbiguityPenalty + ", not " + AOrCToAPenalty);
    }

  }

  private void fail(String message) {
    Assert.fail(message);
  }
}
