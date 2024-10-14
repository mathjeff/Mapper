package mapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

// A HashBlock_Aligner aligns two sequences. It splits sequences into small pieces and counts pieces that have no match, to compute bounds on the alignment penalty
public class HashBlock_Aligner implements LocalAligner {
  public HashBlock_Aligner(LocalAligner nextAligner) {
    this.nextAligner = nextAligner;
  }

  public void setLogger(Logger logger) {
    this.logger = logger;
    this.nextAligner.setLogger(logger.incrementScope());
  }

  public SequenceAlignment align(SequenceSection querySection, SequenceSection referenceSection, AlignmentParameters parameters, AlignmentAnalysis alignmentAnalysis) {
    double maxInterestingPenalty = parameters.MaxErrorRate * querySection.getLength();

    if (querySection.getLength() > referenceSection.getLength()) {
      if (logger.getEnabled()) {
        logger.log("HashBlock_Aligner skipping analysis because query longer than reference");
      }
      return this.nextAligner.align(querySection, referenceSection, parameters, alignmentAnalysis);
    }

    PenaltyAnalysis penaltyAnalysis = this.analyzePenalty(querySection, referenceSection, parameters, alignmentAnalysis);

    if (penaltyAnalysis.minPossiblePenalty > maxInterestingPenalty) {
      if (logger.getEnabled()) {
        logger.log("local hashblocks demonstrate no alignment: min possible penalty = " + penaltyAnalysis.minPossiblePenalty + " > " + maxInterestingPenalty);
      }
      return null;
    }

    int offsetWithMostHashblockMatches = penaltyAnalysis.offsetWithMostHashblockMatches;
    int numHashblocksWithBestOffset = penaltyAnalysis.numHashBlockMatchesWithBestOffset;
    AlignmentAnalysis subAnalysis = alignmentAnalysis.child();
    subAnalysis.maxInsertionExtensionPenalty = penaltyAnalysis.maxInsertionExtensionPenalty;
    subAnalysis.maxDeletionExtensionPenalty = penaltyAnalysis.maxDeletionExtensionPenalty;

    double extraPenaltyForMissingAllHashblockMatches = numHashblocksWithBestOffset * parameters.MutationPenalty + penaltyAnalysis.minPossiblePenalty;
    if (logger.getEnabled()) {
      logger.log("Found " + numHashblocksWithBestOffset + " hashblocks with offset " + offsetWithMostHashblockMatches + " If all mismatch, the penalty is " + extraPenaltyForMissingAllHashblockMatches + " (max interesting penalty = " + maxInterestingPenalty + ")");
    }

    if (extraPenaltyForMissingAllHashblockMatches > maxInterestingPenalty) {
      // If all of these hashblocks mismatch, the penalty is too high
      // So, at least one hashblock must match, and we can be confident about the new offset
      subAnalysis.predictedBestOffset = offsetWithMostHashblockMatches;
      subAnalysis.confidentAboutBestOffset = true;
    } else {
      // If we weren't confident about this offset then we can change the predicted offset to our result without losing any confidence
      if (!alignmentAnalysis.confidentAboutBestOffset)
        subAnalysis.predictedBestOffset = offsetWithMostHashblockMatches;
    }
    // If we were originally confident about this offset and didn't change the offset, we can still be confident about it
    if (alignmentAnalysis.confidentAboutBestOffset && subAnalysis.predictedBestOffset == alignmentAnalysis.predictedBestOffset)
      subAnalysis.confidentAboutBestOffset = true;
    SequenceSection referenceSubsection;
    if (subAnalysis.confidentAboutBestOffset) {
      int maxDeletionLength = (int)((double)penaltyAnalysis.maxDeletionExtensionPenalty / (double)parameters.DeletionExtension_Penalty);
      int maxInsertionLength = (int)((double)penaltyAnalysis.maxInsertionExtensionPenalty / (double)parameters.InsertionExtension_Penalty);
      int maxIndelLength = Math.max(maxDeletionLength, maxInsertionLength);
      int referenceStart = Math.max(referenceSection.getStartIndex(), querySection.getStartIndex() + subAnalysis.predictedBestOffset - maxIndelLength);
      int referenceEnd = Math.min(referenceSection.getEndIndex(), querySection.getEndIndex() + subAnalysis.predictedBestOffset + maxIndelLength);
      referenceSubsection = new SequenceSection(referenceSection.getSequence(), referenceStart, referenceEnd);
      if (logger.getEnabled()) {
        logger.log("Shrinking candidate reference to be within indel length of " + maxIndelLength + ", to " + referenceSubsection.format());
      }
    } else {
      referenceSubsection = referenceSection;
    }
    if (referenceSubsection.getLength() < referenceSection.getLength())
      return this.align(querySection, referenceSubsection, parameters, subAnalysis);
    return this.nextAligner.align(querySection, referenceSubsection, parameters, subAnalysis);
  }

  private boolean isTooManyMismatches(int numMismatches, AlignmentParameters parameters, double maxInterestingPenalty) {
    double indelPenalty = getMinIndelPenaltyForBlockMismatches(numMismatches, parameters);
    if (indelPenalty > maxInterestingPenalty) {
      if (logger.getEnabled()) {
        logger.log("Stopping searching for mismatched miniblocks: " + numMismatches + " mismatches indicates indel penalty " + indelPenalty + ", > " + maxInterestingPenalty);
      }
      return true;
    }
    return false;
  }

  private PenaltyAnalysis analyzePenalty(SequenceSection querySection, SequenceSection referenceSection, AlignmentParameters parameters, AlignmentAnalysis alignmentAnalysis) {

    Sequence query = querySection.getSequence();
    Sequence reference = referenceSection.getSequence();

    HashBlock_Matcher matcher = alignmentAnalysis.hashBlock_matcher;
    double maxInterestingPenalty = parameters.MaxErrorRate * querySection.getLength();

    double maxExtensionPenalty = maxInterestingPenalty - (double)Math.max(parameters.getStartingInsertionStartPenalty(), parameters.DeletionStart_Penalty);
    int maxIndelLength = (int)((double)maxExtensionPenalty / (double)parameters.DeletionExtension_Penalty);
    int numMismatches = 0;
    int maxNonmatchingBlockEnd = querySection.getStartIndex(); // the end index (exclusive) of the previous mismatched block

    CountMap counts = new CountMap();
    int numLateBlocksSupportingInsertion = 0;
    int numLateBlocksSupportingDeletion = 0;
    int minPossibleOffset = referenceSection.getStartIndex() - querySection.getStartIndex();
    int maxPossibleOffset = referenceSection.getEndIndex() - querySection.getEndIndex();

    int lookupUncertainty = maxPossibleOffset - minPossibleOffset;
    if (matcher == null || Math.abs(matcher.getSectionLength() - lookupUncertainty) > lookupUncertainty / 2) {
      if (logger.getEnabled()) {
        logger.log("Creating new HashBlock_Matcher with section length " + lookupUncertainty);
      }
      matcher = new HashBlock_Matcher(query, referenceSection, lookupUncertainty);
      if (alignmentAnalysis.hashBlock_matcher == null)
        alignmentAnalysis.hashBlock_matcher = matcher;
    }

    int blockLength = matcher.getBlockLength();
    int maxBlockStart = querySection.getEndIndex() - blockLength;
    for (int blockStartIndex = querySection.getStartIndex(); blockStartIndex <= maxBlockStart; blockStartIndex++) {
      if (blockStartIndex >= maxNonmatchingBlockEnd) {
        // get the entry
        int minInterestingPosition = blockStartIndex + minPossibleOffset;
        int maxInterestingPosition = blockStartIndex + maxPossibleOffset + 1;
        int position = matcher.lookup(blockStartIndex, blockStartIndex + minPossibleOffset, blockStartIndex + maxPossibleOffset + 1);
        int offset = position - blockStartIndex;

        if (getLogBlockMatches()) {
          String refText = matcher.format(position);
          if (position >= 0) {
            refText += " offset = " + offset;
          }
          logger.log(" matcher lookup(" + blockStartIndex + "(" + query.getRange(blockStartIndex, blockLength) + "), " + minInterestingPosition + ", " + maxInterestingPosition + ") = " + refText);
        }

        if (position == HashBlock_Matcher.UNKNOWN || position == HashBlock_Matcher.MULTIPLE_MATCHES)
          continue;
        if (position == HashBlock_Matcher.NO_MATCHES) {
          numMismatches++;
          maxNonmatchingBlockEnd = blockStartIndex + blockLength;
          if (isTooManyMismatches(numMismatches, parameters, maxInterestingPenalty))
            break;
          continue;
        }

        // Found a single match
        // search reverse for a mismatch
        int otherStartIndex = position;
        int reverseCount = Math.min(blockStartIndex - maxNonmatchingBlockEnd, otherStartIndex);
        boolean foundMismatch = false;
        if (!foundMismatch) {
          for (int i = 1; i <= reverseCount; i++) {
            int indexA = blockStartIndex - i;
            int indexB = otherStartIndex - i;
            byte encodedCharA = query.encodedCharAt(indexA);
            byte encodedCharB = reference.encodedCharAt(indexB);
            if (!Basepairs.canMatch(encodedCharA, encodedCharB)) {
              if (getLogBlockMatches()) {
                logger.log("  reverse search found mismatch at " + indexA + " (" + Basepairs.decode(encodedCharA) + ":" + Basepairs.decode(encodedCharB) + ")");
              }
              numMismatches++;
              foundMismatch = true;
              maxNonmatchingBlockEnd = blockStartIndex + blockLength;
              break;
            }
          }
        }
        if (!foundMismatch) {
          // check for upcoming mismatches
          int forwardShift = querySection.getEndIndex() - blockStartIndex;
          for (int i = blockLength; i < forwardShift; i++) {
            int indexA = blockStartIndex + i;
            int indexB = otherStartIndex + i;
            boolean match;
            byte encodedCharA = query.encodedCharAt(indexA);
            byte encodedCharB;
            if (indexB < referenceSection.getEndIndex())
              encodedCharB = reference.encodedCharAt(indexB);
            else
              encodedCharB = 0;
            if (!Basepairs.canMatch(encodedCharA, encodedCharB)) {
              if (getLogBlockMatches()) {
                logger.log("  forward search found mismatch at " + indexA + " (" + Basepairs.decode(encodedCharA) + ":" + Basepairs.decode(encodedCharB) + ")");
              }
              numMismatches++;
              foundMismatch = true;
              maxNonmatchingBlockEnd = indexA + 1;
              break;
            }
          }
          if (!foundMismatch) {
            maxNonmatchingBlockEnd = querySection.getEndIndex();
          }
          // count the number of other hashblocks in this span that only match here, to use as evidence that the best alignment must touch this offset
          int numOtherContainedUniqueHashblockMatches = 0;
          int forwardShift2 = maxNonmatchingBlockEnd - blockStartIndex - blockLength;
          for (int i = blockLength; i < forwardShift2; i++) {
            int indexA = blockStartIndex + i;
            int minInterestingPosition2 = indexA + minPossibleOffset;
            int maxInterestingPosition2 = indexA + maxPossibleOffset + 1;
            int lookupResult = matcher.lookup(indexA, minInterestingPosition2, maxInterestingPosition2);
            int offset2 = lookupResult - indexA;

            if (lookupResult >= 0 && offset2 == offset) {
              // This block only matches at the same location
              if (getLogBlockMatches()) {
                logger.log("  found block at " + indexA + " matching " + matcher.format(lookupResult) + " supporting offset " + offset2);
              }
              numOtherContainedUniqueHashblockMatches++;
              i = i - 1 + blockLength;
            }
          }
          if (offset != counts.getMostPopularKey() && counts.getMaxPopularity() > 0) {
            if (offset > counts.getMostPopularKey()) {
              numLateBlocksSupportingDeletion += numOtherContainedUniqueHashblockMatches;
            } else {
              numLateBlocksSupportingInsertion += numOtherContainedUniqueHashblockMatches;
            }
          } else {
            // TODO: clear numLateBlocks* under the right conditions
          }
          counts.add(offset, numOtherContainedUniqueHashblockMatches);
        }

        if (foundMismatch) {
          if (isTooManyMismatches(numMismatches, parameters, maxInterestingPenalty))
            break;
        } else {
          // If we're not using this hashblock as evidence of a mismatch, we can use it as evidence that this is the best offset for an alignment
          counts.add(offset, 1);
        }

      }
    }

    int mostPopularOffset = counts.getMostPopularKey();
    int mostPopularOffset_count = counts.getMaxPopularity();
    if (this.logger.getEnabled()) {
      this.logger.log("Mini blocks result: " + numMismatches + " match nowhere; " + mostPopularOffset_count + " match at offset " + mostPopularOffset);
    }

    PenaltyAnalysis result = new PenaltyAnalysis();
    // penalty with indels
    double indelPenalty = this.getMinIndelPenaltyForBlockMismatches(numMismatches, parameters);
    result.minPossiblePenalty = indelPenalty;
    // penalty of a 1-1 alignment that we might not have found yet
    boolean couldBestOffsetBeDifferentThanPreviouslyExpected = mostPopularOffset_count < 1 || alignmentAnalysis.lastCheckedOffset != mostPopularOffset;
    if (couldBestOffsetBeDifferentThanPreviouslyExpected) {
      double mismatchPenalty = numMismatches * parameters.MutationPenalty;
      if (result.minPossiblePenalty > mismatchPenalty)
        result.minPossiblePenalty = mismatchPenalty;
    }

    this.setMaxExtensionPenalty(numMismatches, numLateBlocksSupportingInsertion, numLateBlocksSupportingDeletion, maxInterestingPenalty, parameters, blockLength, result);
    if (result.maxInsertionExtensionPenalty > alignmentAnalysis.maxInsertionExtensionPenalty) {
      if (logger.getEnabled())
        logger.log("lowering max insertion extension penalty to previously computed limit of " + alignmentAnalysis.maxInsertionExtensionPenalty);
      result.maxInsertionExtensionPenalty = alignmentAnalysis.maxInsertionExtensionPenalty;
    }
    if (result.maxDeletionExtensionPenalty > alignmentAnalysis.maxDeletionExtensionPenalty) {
      if (logger.getEnabled())
        logger.log("lowering max deletion extension penalty to previously computed limit of " + alignmentAnalysis.maxDeletionExtensionPenalty);
      result.maxDeletionExtensionPenalty = alignmentAnalysis.maxDeletionExtensionPenalty;
    }

    // keep the previous offset if we didn't find any evidence of another offset
    if (mostPopularOffset_count < 1)
      mostPopularOffset = alignmentAnalysis.predictedBestOffset;
    if (logger.getEnabled()) {
      if (alignmentAnalysis.predictedBestOffset != mostPopularOffset) {
        logger.log("Changing predicted best offset from " + alignmentAnalysis.predictedBestOffset + " to " + mostPopularOffset + " (supported by " + mostPopularOffset_count + " mini blocks)");
      }
    }
    result.offsetWithMostHashblockMatches = mostPopularOffset;
    result.numHashBlockMatchesWithBestOffset = mostPopularOffset_count;

    return result;
  }

  // gets the minimum penalty of an alignment having at least one indel given this number of distinct nonmatching HashBlocks
  private double getMinIndelPenaltyForBlockMismatches(int numMismatches, AlignmentParameters parameters) {
    numMismatches = Math.max(1, numMismatches);

    double minPenaltyPerInitialIndel = Math.min(parameters.getStartingInsertionStartPenalty() + parameters.InsertionExtension_Penalty, parameters.DeletionStart_Penalty + parameters.DeletionExtension_Penalty);
    double minPenaltyPerInitialChange = Math.min(parameters.MutationPenalty, minPenaltyPerInitialIndel);

    double minPenaltyPerExtension = Math.min(parameters.InsertionExtension_Penalty, parameters.DeletionExtension_Penalty);

    double minPenaltyPerSubsequentIndel = Math.min(parameters.InsertionStart_Penalty + parameters.InsertionExtension_Penalty, parameters.DeletionStart_Penalty + parameters.DeletionExtension_Penalty);
    double minPenaltyPerSubsequentChange = Math.min(parameters.MutationPenalty, minPenaltyPerSubsequentIndel);

    // For each nonmatching HashBlock, there must be one of:
    // A) A SNP somewhere in that HashBlock
    // B) An new indel somewhere in that HashBlock
    // C) A previous indel that extended into this HashBlock
    // For the moment we disregard C because it's unlikely to matter and it's inconvenient to calculate

    // The total penalty of an alignment having at least one indel and the given number of changes is:
    // the penalty of one indel plus the penalty of the remaining changes
    if (numMismatches <= 1)
      return minPenaltyPerInitialIndel;
    if (numMismatches <= 2)
      return minPenaltyPerInitialIndel + minPenaltyPerExtension;
    return minPenaltyPerInitialIndel + minPenaltyPerExtension + (numMismatches - 2) * minPenaltyPerSubsequentChange;
  }

  // returns the maximum total penalty that can be spent on extensions given the number of mismatched hashblocks and the total allowable penalty
  private void setMaxExtensionPenalty(int numMismatches, int numBlocksSupportingInsertion, int numBlocksSupportingDeletion, double totalPenalty, AlignmentParameters parameters, int blockLength, PenaltyAnalysis penaltyAnalysis) {
    double longInsertion = getMaxExtensionPenaltyOfLongInsertion(numMismatches + numBlocksSupportingDeletion, totalPenalty, parameters, blockLength);
    double manyInsertions = getMaxExtensionPenaltyOfManyInsertions(numMismatches + numBlocksSupportingInsertion, totalPenalty, parameters, blockLength);
    penaltyAnalysis.maxInsertionExtensionPenalty = Math.max(longInsertion, manyInsertions);

    penaltyAnalysis.maxDeletionExtensionPenalty = getMaxExtensionPenaltyOfManyDeletions(numMismatches + numBlocksSupportingInsertion, totalPenalty, parameters, blockLength);
  }


  private double getMaxExtensionPenaltyOfLongInsertion(int numMismatches, double totalPenalty, AlignmentParameters parameters, int blockLength) {
    double availablePenalty = totalPenalty - parameters.getStartingInsertionStartPenalty();
    double penaltyOfOnlySNPs = numMismatches * parameters.MutationPenalty;
    double penaltyPerBlockExtension = blockLength * parameters.InsertionExtension_Penalty;
    double extraPenaltyPerBlockExtension = penaltyPerBlockExtension - parameters.MutationPenalty;
    if (extraPenaltyPerBlockExtension <= 0) {
      if (logger.getEnabled())
        logger.log("max extension penalty of long insertion = " + availablePenalty + " because extra penalty per block extension " + extraPenaltyPerBlockExtension + " < 0");
      return availablePenalty;
    }
    if (numMismatches < 2) {
      if (logger.getEnabled())
        logger.log("max extension penalty of long insertion = " + availablePenalty + " because numMismatches " + numMismatches + " < 2");
      return availablePenalty;
    }
    double penaltyOfShortExtension = 2 * parameters.InsertionExtension_Penalty;
    if (penaltyOfShortExtension > availablePenalty) {
      if (logger.getEnabled())
        logger.log("max extension penalty of long insertion = " + availablePenalty + " because penalty of short extension " + penaltyOfShortExtension + " > available penalty " + availablePenalty);
      return availablePenalty;
    }
    double penaltyOfShortSNPs = 2 * parameters.MutationPenalty;
    double maxAllowedPenaltyIncreasePastAllSNPs = availablePenalty - penaltyOfOnlySNPs;
    double maxAllowedPenaltyForBlockExtensions = maxAllowedPenaltyIncreasePastAllSNPs + penaltyOfShortSNPs - penaltyOfShortExtension;
    double maxNumBlockExtensions = maxAllowedPenaltyForBlockExtensions / extraPenaltyPerBlockExtension;
    double maxExtensionPenaltyOfLongInsertion = (maxNumBlockExtensions * blockLength + 2) * parameters.InsertionExtension_Penalty;
    maxExtensionPenaltyOfLongInsertion = Math.min(maxExtensionPenaltyOfLongInsertion, availablePenalty);
    if (maxExtensionPenaltyOfLongInsertion < penaltyOfShortExtension)
      maxExtensionPenaltyOfLongInsertion = 0; // cannot have a fraction of an indel
    if (logger.getEnabled())
      logger.log("max extension penalty of long insertion = " + maxExtensionPenaltyOfLongInsertion + " because: available penalty other than indel start = " + availablePenalty + ", " + numMismatches + " mismatches uses " + penaltyOfOnlySNPs + " penalty, max allowed extra penalty beyond indel start plus SNPs = " + maxAllowedPenaltyIncreasePastAllSNPs + ", penalty of 2 SNPs = " + penaltyOfShortSNPs + " extension penalty of length-2 indel = " + penaltyOfShortExtension + ", max allowed penalty for block extensions = " + maxAllowedPenaltyForBlockExtensions + ", penalty per (length " + blockLength + ") block extension = " + penaltyPerBlockExtension + " , extra penalty per block extension = " + extraPenaltyPerBlockExtension + ", max num block extensions = " + maxNumBlockExtensions);
    return maxExtensionPenaltyOfLongInsertion;
  }

  private double getMaxExtensionPenaltyOfManyInsertions(int numMismatches, double totalPenalty, AlignmentParameters parameters, int blockLength) {
    double availablePenalty = totalPenalty + (parameters.InsertionStart_Penalty - parameters.getStartingInsertionStartPenalty());
    double penaltyOfOnlySNPs = numMismatches * parameters.MutationPenalty;
    double penaltyPerShortIndel = parameters.InsertionStart_Penalty + 2 * parameters.InsertionExtension_Penalty;
    double extraPenaltyPerShortIndel = penaltyPerShortIndel - 2 * parameters.MutationPenalty;

    if (extraPenaltyPerShortIndel <= 0) {
      if (logger.getEnabled())
        logger.log("max extension penalty of many insertions = " + availablePenalty + " because extra penalty per indel = " + extraPenaltyPerShortIndel);
      return availablePenalty;
    }

    double maxNumShortIndels = (availablePenalty - penaltyOfOnlySNPs) / extraPenaltyPerShortIndel;
    if (maxNumShortIndels < 1)
      maxNumShortIndels = 0; // cannot have a fraction of an indel
    double result = maxNumShortIndels * 2 * parameters.InsertionExtension_Penalty;
    result = Math.min(result, availablePenalty);
    if (logger.getEnabled())
      logger.log("max extension penalty of many insertions = " + result);
    return result;
  }

  private double getMaxExtensionPenaltyOfManyDeletions(int numMismatches, double totalPenalty, AlignmentParameters parameters, int blockLength) {
    double availablePenalty = totalPenalty;
    double penaltyOfOnlySNPs = numMismatches * parameters.MutationPenalty;
    double penaltyPerShortIndel = parameters.DeletionStart_Penalty + 2 * parameters.DeletionExtension_Penalty;
    double extraPenaltyPerShortIndel = penaltyPerShortIndel - 2 * parameters.MutationPenalty;

    if (extraPenaltyPerShortIndel <= 0) {
      if (logger.getEnabled())
        logger.log("max extension penalty of many deletions = " + availablePenalty + " because extra penalty per indel = " + extraPenaltyPerShortIndel);
      return availablePenalty;
    }

    double maxNumShortIndels = (availablePenalty - penaltyOfOnlySNPs) / extraPenaltyPerShortIndel;
    if (maxNumShortIndels < 1)
      maxNumShortIndels = 0; // cannot have a fraction of an indel
    double result = maxNumShortIndels * 2 * parameters.DeletionExtension_Penalty;
    result = Math.min(result, availablePenalty);
    if (result < 0)
      result = 0;
    if (logger.getEnabled())
      logger.log("max extension penalty of many deletions = " + result);
    return result;
  }

  private boolean getLogBlockMatches() {
    return false; //logger.getEnabled();
  }

  private Logger logger;
  private LocalAligner nextAligner;
  private boolean logBlockMatches;
}
