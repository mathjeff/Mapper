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
    if (!canMatch(encodedReference, encodedQuery)) {
      // if the two basepairs don't match, that's a mutation
      return alignmentParameters.MutationPenalty;
    }
    // if the two basepairs can match, we still have to calculate a penalty if there is ambiguity
    return alignmentParameters.AmbiguityPenalty * getMutationFalseNegativeRate(encodedReference);
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
