package mapper;

public class StraightAligner implements LocalAligner {
  public StraightAligner(LocalAligner nextAligner) {
    this.nextAligner = nextAligner;
  }

  public void setLogger(Logger logger) {
    this.logger = logger;
    this.nextAligner.setLogger(logger.incrementScope());
  }

  public SequenceAlignment align(SequenceSection querySection, SequenceSection referenceSection, AlignmentParameters parameters, AlignmentAnalysis alignmentAnalysis) {
    alignmentAnalysis.lastCheckedOffset = alignmentAnalysis.predictedBestOffset;
    SequenceAlignment simpleAlignment = straightAlignment(querySection, referenceSection, parameters, alignmentAnalysis);
    double simpleAlignment_totalPenalty = simpleAlignment.getAlignedPenalty();
    double maxInterestingPenalty = querySection.getLength() * parameters.MaxErrorRate;

    double indelPenalty = Math.min(parameters.getStartingInsertionStartPenalty() + parameters.InsertionExtension_Penalty, parameters.DeletionStart_Penalty + parameters.DeletionExtension_Penalty);
    if (logger.getEnabled()) {
      logger.log("1-1 alignment for query" + querySection.formatRange() + " at offset " + alignmentAnalysis.predictedBestOffset + ":");
      logger.log(" query: " + simpleAlignment.getAlignedTextA());
      logger.log("   ref: " + simpleAlignment.getAlignedTextB());
    }
    if (simpleAlignment_totalPenalty <= 0) {
      if (logger.getEnabled()) {
        logger.log("Using 1-1 alignment with penalty " + simpleAlignment_totalPenalty);
      }
      return simpleAlignment;
    }

    if (alignmentAnalysis.confidentAboutBestOffset) {
      if (simpleAlignment_totalPenalty <= indelPenalty || (alignmentAnalysis.maxInsertionExtensionPenalty <= 0 && alignmentAnalysis.maxDeletionExtensionPenalty <= 0)) {
        if (simpleAlignment_totalPenalty <= maxInterestingPenalty) {
          if (logger.getEnabled()) {
            logger.log("Using 1-1 alignment with penalty " + simpleAlignment_totalPenalty + ", less than an indel");
          }
          return simpleAlignment;
        } else {
          if (logger.getEnabled()) {
            logger.log("No alignment: 1-1 alignment penalty " + simpleAlignment_totalPenalty + " is less than an indel and still higher than requested " + maxInterestingPenalty);
          }
          return null;
        }
      }

      if (indelPenalty > maxInterestingPenalty) {
        if (logger.getEnabled()) {
          logger.log("No alignment: 1-1 alignment penalty " + simpleAlignment_totalPenalty + " is more than an indel which is more than requested " + maxInterestingPenalty);
        }
        return null;
      }
    }

    if (logger.getEnabled()) {
      logger.log("Penalty of 1-1 alignment = " + simpleAlignment_totalPenalty + "; checking whether indels can lower the penalty");
    }
    double simpleAlignment_penaltyRate = simpleAlignment.getAlignedPenalty() / querySection.getLength();
    AlignmentParameters subParameters = parameters.clone();
    subParameters.MaxErrorRate = Math.min(simpleAlignment_penaltyRate, parameters.MaxErrorRate);
    SequenceAlignment alignment = this.nextAligner.align(querySection, referenceSection, subParameters, alignmentAnalysis);
    // break ties in favor of having no indels
    if (alignment == null || alignment.getAlignedPenalty() >= simpleAlignment_totalPenalty) {
      if (simpleAlignment_totalPenalty <= maxInterestingPenalty) {
        if (logger.getEnabled())
          logger.log("using 1-1 alignment with penalty " + simpleAlignment_totalPenalty);
        return simpleAlignment;
      }
    }
    return alignment;
  }

  private SequenceAlignment straightAlignment(SequenceSection querySection, SequenceSection referenceSection, AlignmentParameters parameters, AlignmentAnalysis alignmentAnalysis) {
    int queryStartIndex = querySection.getStartIndex();
    int queryEndIndex = querySection.getEndIndex();
    int referenceStartIndex = referenceSection.getStartIndex();
    int referenceEndIndex = referenceSection.getEndIndex();
    int predictedBestOffset = alignmentAnalysis.predictedBestOffset;
    // clamp ranges
    if (queryStartIndex + predictedBestOffset > referenceStartIndex) {
      referenceStartIndex = queryStartIndex + predictedBestOffset;
    } else {
      queryStartIndex = referenceStartIndex - predictedBestOffset;
    }
    if (queryEndIndex + predictedBestOffset < referenceEndIndex) {
      referenceEndIndex = queryEndIndex + predictedBestOffset;
    } else {
      queryEndIndex = referenceEndIndex - predictedBestOffset;
    }
    Sequence query = querySection.getSequence();
    Sequence reference = referenceSection.getSequence();

    return new SequenceAlignment(new AlignedBlock(query, reference, queryStartIndex, referenceStartIndex, queryEndIndex - queryStartIndex, referenceEndIndex - referenceStartIndex), parameters, (query.getComplementedFrom() != null));
  } 


  Logger logger;
  LocalAligner nextAligner;

}
