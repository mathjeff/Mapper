package mapper;

import java.util.List;

// A SequencesIterator provides a list of sequences
public class SequencesIterator implements SequenceProvider {
  public SequencesIterator(List<SequenceProvider> providers) {
    this.providers = providers;
  }

  public SequenceBuilder getNextSequence() {
    while (this.nextIndex < this.providers.size()) {
      SequenceBuilder next = this.providers.get(this.nextIndex).getNextSequence();
      if (next != null) {
        return next;
      }
      this.nextIndex++;
    }
    return null;
  }

  public boolean get_allReadsContainQualityInformation() {
    for (SequenceProvider provider : this.providers) {
      if (!provider.get_allReadsContainQualityInformation()) {
        return false;
      }
    }
    return true;
  }

  int nextIndex;
  List<SequenceProvider> providers;
}
