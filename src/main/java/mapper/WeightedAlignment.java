package mapper;

// A WeightedAlignment represents a SequenceAlignment plus a weight at each position
class WeightedAlignment {
  public WeightedAlignment(SequenceAlignment sequenceAlignment, QueryAlignment queryAlignment, float weight) {
    this.sequenceAlignment = sequenceAlignment;
    this.queryAlignment = queryAlignment;
    this.overallWeight = weight;
  }

  public SequenceAlignment getAlignment() {
    return this.sequenceAlignment;
  }

  public QueryAlignment getQueryAlignment() {
    return this.queryAlignment;
  }

  public float getWeight(int referenceIndex) {
    float numAlignmentsHere = queryAlignment.getNumAlignmentsCoveringIndexB(referenceIndex);
    float positionWeight;
    if (numAlignmentsHere != 0) {
      positionWeight = (float)1.0 / numAlignmentsHere;
    } else {
      positionWeight = 0;
    }
    return this.overallWeight * positionWeight;
  }

  private SequenceAlignment sequenceAlignment;
  private QueryAlignment queryAlignment;
  private float overallWeight;
}
