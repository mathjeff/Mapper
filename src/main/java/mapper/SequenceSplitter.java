package mapper;

public class SequenceSplitter implements SequenceProvider {
  public SequenceSplitter(int maxLength, SequenceProvider provider) {
    this.maxLength = maxLength;
    this.provider = provider;
  }

  public SequenceBuilder getNextSequence() {
    if (this.numSectionsConsumed >= this.numSections) {
      SequenceBuilder sequenceBuilder = this.provider.getNextSequence();
      if (sequenceBuilder == null) {
        this.pendingSequence = null;
        return null;
      }
      this.pendingSequence = sequenceBuilder.build();
      this.numSections = (this.pendingSequence.getLength() - 1) / this.maxLength + 1;
      this.numSectionsConsumed = 0;
    }
    int startIndex = this.getStartIndex();
    this.numSectionsConsumed++;
    int endIndex = this.getStartIndex();
    Sequence nextSequence = this.pendingSequence.getSubsequence((int)startIndex, (int)(endIndex - startIndex));

    // convert from Sequence back to SequenceBuilder
    // TODO: make this faster if it's important
    SequenceBuilder builder = new SequenceBuilder();
    builder.setName(nextSequence.getName() + "[" + startIndex + ":" + endIndex + "]");
    builder.setPath(nextSequence.getPath());
    builder.add(nextSequence.getText());
    return builder;
  }

  // gets the start index of the next subsequence
  private int getStartIndex() {
    // use longs for computation to avoid overflow
    return (int)((long)this.pendingSequence.getLength() * (long)this.numSectionsConsumed / (long)this.numSections);
  }

  public boolean get_allReadsContainQualityInformation() {
    // not implemented
    return false;
  }

  @Override
  public String toString() {
    return "" + this.provider + " split to size <= " + this.maxLength;
  }

  Sequence pendingSequence;
  int numSections;
  int numSectionsConsumed;

  SequenceProvider provider;
  int maxLength;
}
