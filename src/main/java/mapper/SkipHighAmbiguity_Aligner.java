package mapper;

public class SkipHighAmbiguity_Aligner implements LocalAligner {
  public SkipHighAmbiguity_Aligner(LocalAligner nextAligner) {
    this.nextAligner = nextAligner;
  }

  public void setLogger(Logger logger) {
    this.logger = logger;
    this.nextAligner.setLogger(logger);
  }

  public SequenceAlignment align(SequenceSection querySection, SequenceSection referenceSection, AlignmentParameters parameters, AlignmentAnalysis alignmentAnalysis) {
    int numAmbiguities = 0;
    Sequence reference = referenceSection.getSequence();
    for (int i = referenceSection.getStartIndex(); i < referenceSection.getEndIndex(); i++) {
      if (Basepairs.isAmbiguous(reference.charAt(i))) {
        numAmbiguities++;
      }
    }
    if (numAmbiguities >= referenceSection.getLength() / 4) {
      if (this.logger.getEnabled()) {
        this.logger.log("Skipping checking for indels due to high number of ambiguities (" + numAmbiguities + ") among " + referenceSection.getLength() + " basepairs");
      }
      return null;
    }
    return this.nextAligner.align(querySection, referenceSection, parameters, alignmentAnalysis);
  }

  Logger logger;
  LocalAligner nextAligner;
}
