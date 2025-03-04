package mapper;

// Basepairs compares basepairs
// It can compare unambiguous basepairs like 'A', 'C', 'G', 'T'
// It can also compare ambiguous basepairs like 'N'

// basepairs are encoded using 4 bits. Each bit specifies whether that basepair can be that letter.
public class Basepairs {

  // Compute encoded values
  // A = 1
  // C = 2
  // G = 4
  // T = 8
  // 1  -> A
  // 2  -> C
  // 3  -> C|A = M
  // 4  -> G
  // 5  -> G|A = R
  // 6  -> G|C = S
  // 7  -> G|C|A = V
  // 8  -> T
  // 9  -> T|A = W
  // 10 -> T|C = Y
  // 11 -> T|C|A = H
  // 12 -> T|G = K
  // 13 -> T|G|A = D
  // 14 -> T|G|C = B
  // 15 -> T|G|C|A = N
  private static String allEncoded = "-ACMGRSVTWYHKDBN";

  public static char decode(byte encoded) {
    return allEncoded.charAt(encoded);
  }

  public static byte encode(char item) {
    switch (item) {
      case '-':
        return 0;
      case 'A':
        return 1;
      case 'C':
        return 2;
      case 'M':
        return 3;
      case 'G':
        return 4;
      case 'R':
        return 5;
      case 'S':
        return 6;
      case 'V':
        return 7;
      case 'T':
        return 8;
      case 'W':
        return 9;
      case 'Y':
        return 10;
      case 'H':
        return 11;
      case 'K':
        return 12;
      case 'D':
        return 13;
      case 'B':
        return 14;
      case 'N':
        return 15;
      default:
    }
    throw new IllegalArgumentException("Cannot encode " + item + " as a basepair");
  }

  // given two encoded basepairs, returns the union
  // For example, given A and C, returns A|C which is M.
  public static byte union(byte encoded1, byte encoded2) {
    return (byte)(encoded1 | encoded2);
  }

  // tells whether there is any specific allele that both of these potentially ambiguous encoded basepairs can match
  public static boolean canMatch(byte encoded1, byte encoded2) {
    return (encoded1 & encoded2) != 0;
  }

  // Returns the probability that we would fail to detect a mutation from this item
  // The more ambiguous the item is, the fewer mutations we can detect
  public static double getMutationFalseNegativeRate(byte item) {
    return (countNumChoices(item) - 1.0) / 3.0;
  }

  public static double getPenalty(byte encodedQuery, byte encodedReference, AlignmentParameters alignmentParameters) {
    // The penalty of an alignment is essentially the log of how likely we think it is to observe this alignment
    if (!canMatch(encodedReference, encodedQuery)) {
      // if the two basepairs don't match, that's a mutation, and we know the penalty
      return alignmentParameters.MutationPenalty;
    }
    // if the two basepairs can match, we have to calculate a penalty if there is ambiguity
    // We want to calculate the probability that we could identify a mutation in one of these basepairs

    // Suppose that the possible values reported for the query must be one of:
    //  - The ambiguity code we have for it now
    //  - Any other specific basepair that doesn't match that ambiguity code
    // Suppose that the possible values reported for the reference must be one of:
    //  - The ambiguity code we have for it now
    //  - Any other specific basepair that doesn't match that ambiguity code

    // This means:
    //  - If the query's true value mutates into another specific basepair covered by its current ambiguity code, we expect it to be still reported via the same ambiguity code and we expect to not be able to detect this mutation
    //  - If the query's true value mutates into another specific basepair covered by the reference's ambiguity code, we would still consider this to be a match and wouldn't notice the mutation
    //  - If the query's true value mutates into another specific basepair that isn't covered by the query's current ambiguity code or the reference's current ambiguity code, then we could detect this mutation

    // So, we want to compute the probability that a mutation in one of these basepairs would change it into a basepair that isn't included in the either the query's current ambiguity code or the reference's ambiguity code
    byte encodedUnion = union(encodedQuery, encodedReference);
    return alignmentParameters.AmbiguityPenalty * getMutationFalseNegativeRate(encodedUnion);
  }

  public static byte complement(byte encoded) {
    byte result = 0;
    if ((encoded & 8) != 0)
      result += 1;
    if ((encoded & 4) != 0)
      result += 2;
    if ((encoded & 2) != 0)
      result += 4;
    if ((encoded & 1) != 0)
      result += 8;
    return result;
  }

  public static boolean isAmbiguous(byte encoded) {
    return encoded != 0 && encoded != 1 && encoded != 2 && encoded != 4 && encoded != 8;
  }

  public static boolean isAmbiguous(char c) {
    return c != 'A' && c != 'C' && c != 'G' && c != 'T' && c != '-';
  }

  public static boolean isAmbiguous(String s) {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (isAmbiguous(c)) {
        return true;
      }
    }
    return false;
  }

  public static boolean isFullyAmbiguous(byte encoded) {
    return countNumChoices(encoded) > 3;
  }

  // Returns the number of different possible specific letters this basepair could be
  private static int countNumChoices(byte encoded) {
    return (encoded & 8) / 8 + (encoded & 4) / 4 + (encoded & 2) / 2 + (encoded & 1);
  }
}
