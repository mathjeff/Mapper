package mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

// A HashBlock_Matcher matches two sequences and can tell which parts of one sequence match which parts of another
public class HashBlock_Matcher {
  public static int NO_MATCHES = -1;
  public static int MULTIPLE_MATCHES = -2;
  public static int UNKNOWN = -3;

  public HashBlock_Matcher(Sequence query, SequenceSection referenceSection, int sectionLength) {
    if (sectionLength < 1)
      sectionLength = 1;
    this.blockLength = (int)(Math.log(sectionLength * 5) / Math.log(4) + 1);
    if (this.blockLength < 3)
      this.blockLength = 3; // with the way we interpret the results currently, it's more useful if we have blocks of at least this size
    this.reference = referenceSection.getSequence();
    this.referenceStart = referenceSection.getStartIndex();
    this.referenceLength = referenceSection.getLength();
    this.sectionLength = sectionLength;
    this.query = query;
    this.maxSectionIndex =  this.getSectionIndex(reference.getLength() - 1);
    this.numPossibilities = (int)Math.pow(4, blockLength);
    this.maxPossibility = numPossibilities - 1;
    this.locations = new ArrayList<int[]>();
  }

  public int getBlockLength() {
    return this.blockLength;
  }

  public int getSectionLength() {
    return this.sectionLength;
  }

  // Returns a Map<encoded content, position>
  private int[] indexSection(int sectionIndex, int[] section) {
    for (int i = 0; i < numPossibilities; i++) {
      section[i] = NO_MATCHES;
    }
    int previousEncoded = UNKNOWN;
    int startIndex = this.referenceStart + sectionIndex * this.sectionLength;
    int endIndex = Math.min(startIndex + this.sectionLength, referenceStart + referenceLength - this.blockLength);
    for (int i = startIndex; i < endIndex; i++) {
      int encoded;
      if (previousEncoded == UNKNOWN) {
        // recompute the encoded value
        encoded = encodeBlock(reference, i);
      } else {
        // shift the previous encoded value, which should be faster
        byte nextChar = reference.encodedCharAt(i + this.blockLength - 1);
        if (Basepairs.isAmbiguous(nextChar))
          encoded = UNKNOWN;
        else
          encoded = ((previousEncoded * 4) & maxPossibility) + encodedCharToInt(nextChar);
      }

      if (encoded == UNKNOWN) {
        // if we have an unknown basepair here then we can't be sure about any lookups in this section
        //return null;
        // however, it's also very expensive to check the ambiguous basepair here so we ignore it
        continue;
      } else {
        int existing = section[encoded];
        if (existing == NO_MATCHES) {
          section[encoded] = i;
        } else {
          section[encoded] = MULTIPLE_MATCHES;
        }
      }
      previousEncoded = encoded;
    }
    return section;
  }

  private int encodeBlock(Sequence sequence, int index) {
    if (index + blockLength > sequence.getLength()) {
      return UNKNOWN;
    }
    int sum = 0;
    for (int i = 0; i < blockLength; i++) {
      byte here = sequence.encodedCharAt(index + i);
      if (Basepairs.isAmbiguous(here))
        return UNKNOWN;
      sum = sum * 4 + encodedCharToInt(here);
    }
    return sum;
  }

  public int lookup(int queryIndex) {
    return this.lookup(queryIndex, 0, this.reference.getLength() - 1);
  }

  // finds the position in the reference that matches query[index:index+blockLength]
  public int lookup(int queryIndex, int minReferenceIndex, int maxReferenceIndex) {
    if (minReferenceIndex < 0)
      return UNKNOWN; // the reference sequence we have might not be the true complete reference
    if (maxReferenceIndex > this.reference.getLength())
      return UNKNOWN; // the reference sequence we have might not be the true complete reference

    int encoded = encodeBlock(this.query, queryIndex);
    if (encoded < 0)
      return UNKNOWN;
    int matched = NO_MATCHES;
    int minSectionIndex = Math.max(0, this.getSectionIndex(minReferenceIndex));
    int maxSectionIndex = Math.min(this.maxSectionIndex, this.getSectionIndex(maxReferenceIndex));
    // check each section
    for (int sectionIndex = minSectionIndex; sectionIndex <= maxSectionIndex; sectionIndex++) {
      int[] section = this.getSection(sectionIndex);
      int lookedUp;
      // if the sections are very short, it's faster to not index them, and to fall back to scanSection
      if (this.sectionLength < 3) {
        lookedUp = scanSection(queryIndex, sectionIndex);
      } else {
        if (section != null) {
          lookedUp = section[encoded];
        } else {
          // There is an ambiguity in this section of the reference, so we don't store lookup information for this section and have to re-scan this section
          return UNKNOWN;
        }
      }
      if (lookedUp == UNKNOWN)
        return UNKNOWN;
      if (lookedUp == MULTIPLE_MATCHES)
        return MULTIPLE_MATCHES;
      if (lookedUp == NO_MATCHES)
        continue;
      if (lookedUp < minReferenceIndex || lookedUp > maxReferenceIndex)
        continue; // there's a single match in this section but we're not interested in it
      // we found a single match in this section
      if (matched != NO_MATCHES)
        return MULTIPLE_MATCHES;
      matched = lookedUp;
    }
    if (matched < 0)
      return matched;
    return matched;
  }

  private int scanSection(int queryIndex, int sectionIndex) {
    int result = NO_MATCHES;
    int startIndex = this.referenceStart + sectionIndex * this.sectionLength;
    int endIndex = startIndex + this.sectionLength;
    for (int i = startIndex; i < endIndex; i++) {
      if (this.canPositionsMatch(queryIndex, i)) {
        if (result == NO_MATCHES) {
          result = i;
        } else {
          return MULTIPLE_MATCHES;
        }
      }
    }
    return result;
  }

  private boolean canPositionsMatch(int queryIndex, int referenceIndex) {
    if (referenceIndex + this.blockLength > this.referenceStart + this.referenceLength)
      return false;
    for (int i = 0; i < this.blockLength; i++) {
      byte a = this.query.encodedCharAt(queryIndex);
      queryIndex++;
      byte b = this.reference.encodedCharAt(referenceIndex);
      referenceIndex++;
      if (!Basepairs.canMatch(a, b))
        return false;
    }
    return true;
  }

  public String format(int position) {
    if (position == UNKNOWN)
      return "unknown";
    if (position == NO_MATCHES)
      return "none";
    if (position == MULTIPLE_MATCHES)
      return "multiple";
    return "" + position + "(" + this.reference.getRange(position, this.blockLength) + ")";
  }

  // converts from Basepairs encoding to just a smaller A/C/G/T encoding
  private int encodedCharToInt(byte b) {
    switch (b) {
      case 1:
        return 0;
      case 2:
        return 1;
      case 4:
        return 2;
      case 8:
        return 3;
      default:
        throw new IllegalArgumentException("invalid encoded char " + b);
    }
  }

  private int getSectionIndex(int referenceIndex) {
    return (referenceIndex - this.referenceStart) / this.sectionLength;
  }

  private int[] getSection(int index) {
    // if we already indexed this section, we can reuse it
    if (this.locations.size() > index)
      return this.locations.get(index);

    // allocate jump to this section and index just this section
    while (this.locations.size() <= index) {
      this.locations.add(null);
    }
    int[] section = new int[this.numPossibilities];
    this.locations.set(index, this.indexSection(index, section));
    return this.locations.get(index);
  }

  private Sequence query;
  private Sequence reference;
  private List<int[]> locations;
  private int referenceStart;
  private int referenceLength;
  private int blockLength;
  private int sectionLength;
  private int maxSectionIndex;
  private int numPossibilities;
  private int maxPossibility;
}
