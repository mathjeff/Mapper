package mapper;

import java.util.List;

// AlignmentAnalysis keeps track of what we know about an alignment
public class AlignmentAnalysis {
  private static double large = 1000000;

  public HashBlock_Matcher hashBlock_matcher;

  // we believe that the best alignment intersects this alignment
  public int predictedBestOffset;
  // the last value of predictedBestOffset that was checked by StraightAligner
  public int lastCheckedOffset;

  public boolean confidentAboutBestOffset;

  public double maxInsertionExtensionPenalty = large;
  public double maxDeletionExtensionPenalty  = large;

  public AlignmentAnalysis child() {
    AlignmentAnalysis result = new AlignmentAnalysis();
    result.predictedBestOffset = this.predictedBestOffset;
    result.confidentAboutBestOffset = this.confidentAboutBestOffset;
    result.hashBlock_matcher = this.hashBlock_matcher;
    result.maxInsertionExtensionPenalty = this.maxInsertionExtensionPenalty;
    result.maxDeletionExtensionPenalty = this.maxDeletionExtensionPenalty;
    result.lastCheckedOffset = this.lastCheckedOffset;
    return result;
  }
}
