package mapper;

// A LocalAligner aligns a query to a small section of the reference, including finding indels
interface LocalAligner {
  SequenceAlignment align(SequenceSection querySection, SequenceSection referenceSection, AlignmentParameters parameters, AlignmentAnalysis alignmentAnalysis);
  void setLogger(Logger logger);
}
