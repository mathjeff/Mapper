package mapper;

public class MutationDetectionParameters {
  public static MutationDetectionParameters defaultFilter() {
    MutationDetectionParameters result = new MutationDetectionParameters();
    result.minSNPTotalDepth = 5;
    result.minSNPDepthFraction = (float)0.9;

    result.minIndelTotalStartDepth = 1;
    result.minIndelStartDepthFraction = (float)0.8;

    result.minIndelContinuationTotalDepth = 1;
    result.minIndelContinuationDepthFraction = (float)0.7;

    return result;
  }

  public static MutationDetectionParameters emptyFilter() {
    return new MutationDetectionParameters();
  }

  public float minSNPTotalDepth;
  public float minSNPDepthFraction;

  public float minIndelTotalStartDepth;
  public float minIndelStartDepthFraction;

  public float minIndelContinuationTotalDepth;
  public float minIndelContinuationDepthFraction;

  public boolean supportsSNP(float mutationDepth, float totalDepth) {
    if (totalDepth < this.minSNPTotalDepth)
      return false;
    if (mutationDepth <= 0)
      return false;
    float mutationFraction = mutationDepth / totalDepth;
    if (mutationFraction < this.minSNPDepthFraction)
      return false;
    return true;
  }

  public boolean supportsIndelStart(AlignmentPosition frequencies) {
    float middleDepth = frequencies.getMiddleCount();
    if (middleDepth < this.minIndelTotalStartDepth)
      return false;
    float middleIndelDepth;
    float endIndelDepth;
    if (frequencies.getReference() == '-') {
      middleIndelDepth = middleDepth - frequencies.getMiddleReferenceCount();
      endIndelDepth = frequencies.getEndCount() - frequencies.getEndReferenceCount();
    } else {
      middleIndelDepth = frequencies.getMiddleAlternateCount('-');
      endIndelDepth = frequencies.getEndAlternateCount('-');
    }
    if (middleIndelDepth <= 0 && endIndelDepth <= 0)
      return false;
    float indelFraction = middleIndelDepth / middleDepth;
    if (indelFraction < this.minIndelStartDepthFraction)
      return false;
    return true;
  }

  public boolean supportsIndelContinuation(AlignmentPosition frequencies) {
    float middleDepth = frequencies.getMiddleCount();
    if (middleDepth < this.minIndelContinuationTotalDepth)
      return false;

    float middleIndelDepth;
    float endIndelDepth;
    if (frequencies.getReference() == '-') {
      middleIndelDepth = middleDepth - frequencies.getMiddleReferenceCount();
      endIndelDepth = frequencies.getEndCount() - frequencies.getEndReferenceCount();
    } else {
      middleIndelDepth = frequencies.getMiddleAlternateCount('-');
      endIndelDepth = frequencies.getEndAlternateCount('-');
    }

    if (middleIndelDepth <= 0 && endIndelDepth <= 0)
      return false;

    float indelFraction = middleIndelDepth / middleDepth;
    if (indelFraction < this.minIndelContinuationDepthFraction)
      return false;
    return true;
  }


}
