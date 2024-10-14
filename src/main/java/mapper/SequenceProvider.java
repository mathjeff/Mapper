package mapper;

// A SequenceProvider returns Sequence objects
public interface SequenceProvider {
  SequenceBuilder getNextSequence();
  boolean get_allReadsContainQualityInformation();
}
