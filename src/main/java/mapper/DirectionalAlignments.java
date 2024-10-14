package mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// A DirectionalAlignments tells how various Seqeuences align in a specific direction
// One DirectionalAlignments will handle the forward direction and one will handle the reverse direction
public class DirectionalAlignments {
  private static int weightScale = 100;

  public DirectionalAlignments(Sequence sequence) {
    this.referenceCounts = new int[sequence.getLength()];
    this.alternates = new VariantsInsertions[sequence.getLength()];
    this.sequence = sequence;
    this.dashEncoded = Basepairs.encode('-');
  }

  public void add(int referenceIndex, byte encodedValue, float weight, Sequence querySequence, int queryPosition) {
    if (Basepairs.isAmbiguous(encodedValue)) {
      // We're not interested in fully-ambiguous basepairs ("N")
      // We don't currently support partially-ambiguous basepairs ("R")
      return;
    }
    int scaledWeight = (int)(weight * weightScale);
    if (encodedValue == this.sequence.encodedCharAt(referenceIndex)) {
      this.referenceCounts[referenceIndex] += scaledWeight;
    } else {
      Variants variants = this.getOrCreateVariants(referenceIndex);
      char decoded = Basepairs.decode(encodedValue);
      Variant variant = variants.getOrCreate(decoded);
      variant.addCount(scaledWeight);
      if (encodedValue == dashEncoded) {
        queryPosition = -queryPosition; // mark this as an insertion
      }
      this.offerExample(variant, querySequence, queryPosition);
    }
  }

  public void insert(int referenceIndex, String value, float weight, Sequence querySequence, int queryPosition) {
    double rescaledWeight = weight * weightScale;
    VariantsInsertions variantsInsertions = this.getOrCreateVariants(referenceIndex);
    for (int i = 0; i < value.length(); i++) {
      char baseHere = value.charAt(i);
      if (Basepairs.isAmbiguous(baseHere))
        baseHere = 'N'; // we currently don't allocate space to distinguish partially ambiguous basepairs
      byte encodedBaseHere = Basepairs.encode(baseHere);
      Variants variants = variantsInsertions.getOrCreateInsertions(i);
      Variant insertion = variants.getOrCreate(baseHere);
      insertion.addCount((int)rescaledWeight);

      this.offerExample(insertion, querySequence, queryPosition + i);
    }
  }

  private void offerExample(Variant variant, Sequence query, int queryPosition) {
    // negative query positions indicate insertions
    if (this.betterExample(variant, query, queryPosition))
      variant.setExample(query, queryPosition);
  }

  private boolean betterExample(Variant variant, Sequence query, int queryPosition) {
    Sequence existingExample = variant.getExampleSequence();
    if (existingExample == null) {
      // Having an example is better than having no example
      return true;
    }

    if (existingExample.getLength() != query.getLength()) {
      // A longer example is better than a shorter example
      return query.getLength() > existingExample.getLength();
    }

    if (variant.getExampleIndex() != queryPosition) {
      int idealIndex = query.getLength() / 2;
      // Insertions are recorded as negative indices, so we take the absolute value
      int existingDelta = Math.abs(Math.abs(variant.getExampleIndex()) - idealIndex);
      int newDelta = Math.abs(Math.abs(queryPosition) - idealIndex);
      if (existingDelta != newDelta) {
        // A mutation in the middle of a query is better than at the end of the query
        return newDelta < existingDelta;
      }
      // Break ties deterministically by choosing an earlier position in the query
      return queryPosition < variant.getExampleIndex();
    }

    int nameComparison = existingExample.getName().compareTo(query.getName());
    if (nameComparison != 0) {
      // Break ties deterministically using the name of the read
      return nameComparison < 0;
    }

    // If even the query names are the same, use the query id for a tiebreaker
    return query.getId() < existingExample.getId();
  }

  public void updateCount(AlignmentPosition position, int referenceIndex, boolean forward, boolean nearQueryEnd) {
    Variants variants = this.alternates[referenceIndex];
    if (variants != null) {

      int scaledInsertions = 0;
      Variant[] items = variants.getAll();
      if (items != null) {
        for (Variant variant: items) {
          int scaledCount = variant.getCount();
          position.putScaled(variant.getAllele(), scaledCount, forward, nearQueryEnd);
          Sequence variantSequence = variant.getExampleSequence();
          int index = variant.getExampleIndex();
          boolean deletion = index < 0;
          position.putSampleAlternateSequence(variantSequence, Math.abs(index), deletion);
        }
      }
    }
    position.putScaled(this.sequence.charAt(referenceIndex), referenceCounts[referenceIndex], forward, nearQueryEnd);
  }

  private VariantsInsertions getOrCreateVariants(int index) {
    VariantsInsertions variants = this.alternates[index];
    if (variants == null) {
      variants = new VariantsInsertions();
      this.alternates[index] = variants;
    }
    return variants;
  }

  private AlignmentPosition get(int referenceIndex, boolean forward, boolean nearQueryEnd) {
    AlignmentPosition basePosition = new AlignmentPosition(this.sequence.charAt(referenceIndex));
    this.updateCount(basePosition, referenceIndex, forward, nearQueryEnd);
    return basePosition;
  }

  public void updateInsertionCount(AlignmentPosition position, int referenceIndex, int insertionIndex, boolean forward, boolean nearQueryEnd) {
    VariantsInsertions variantsInsertions = this.alternates[referenceIndex];
    int scaledInsertions = 0;
    if (variantsInsertions != null) {
      Variants variants = variantsInsertions.getInsertions(insertionIndex);
      if (variants != null) {
        Variant[] items = variants.getAll();
        if (items != null) {
          for (Variant variant: items) {
            int scaledCount = variant.getCount();
            position.putScaled(variant.getAllele(), scaledCount, forward, nearQueryEnd);
            scaledInsertions += scaledCount;
    
            position.putSampleAlternateSequence(variant.getExampleSequence(), variant.getExampleIndex(), false);
          }
        }
      }
    }

    AlignmentPosition basePosition = get(referenceIndex, forward, nearQueryEnd);
    // Estimate the number of queries that don't have an insertion
    // This can be slightly incorrect if a query doesn't cover the entire insertion
    // In most cases this should be approximately correct, though.
    int scaledNoninsertions = (int)(basePosition.getCount() * weightScale) - scaledInsertions;
    // If we estimated a positive number, then we use it
    if (scaledNoninsertions > 0)
      position.putScaled('-', scaledNoninsertions, forward, nearQueryEnd);
  }

  private int charToIndex(char c) {
    return c;
  }

  // count * multiplier
  private int[] referenceCounts;
  private VariantsInsertions[] alternates;

  private Sequence sequence;

  private byte dashEncoded;
}
