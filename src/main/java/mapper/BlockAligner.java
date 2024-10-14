package mapper;

import java.util.ArrayList;
import java.util.List;

// A BlockAligner aligns two sequences. It splits sequences into small pieces, aligns smaller pieces, and either reassembles the result or retries the alignment with larger pieces
public class BlockAligner implements LocalAligner {
  public BlockAligner(LocalAligner nextAligner) {
    this.nextAligner = nextAligner;
  }

  public void setLogger(Logger logger) {
    this.logger = logger;
    this.nextAligner.setLogger(logger.incrementScope());
  }

  public SequenceAlignment align(SequenceSection querySection, SequenceSection reference, AlignmentParameters parameters, AlignmentAnalysis alignmentAnalysis) {
    double maxInterestingPenalty = parameters.MaxErrorRate * querySection.getLength();
    List<SequenceAlignment> alignments = this.initialAlignments(querySection, reference, parameters, alignmentAnalysis);
    if (alignments == null || alignments.size() < 1) {
      if (this.logger.getEnabled())
        this.logger.log("no initial alignments");
      return null;
    }
    boolean even = false;
    while (alignments.size() > 1) {
      alignments = this.joinAlignments(alignments, reference, parameters, maxInterestingPenalty, alignmentAnalysis, even);
      if (alignments == null) {
        if (this.logger.getEnabled())
          this.logger.log("joined alignments down to null");
        return null;
      }
      even = !even;
    }
    return alignments.get(0);
  }

  // splits the initial match into smaller matches, and aligns each one
  private List<SequenceAlignment> initialAlignments(SequenceSection querySection, SequenceSection referenceSection, AlignmentParameters alignmentParameters, AlignmentAnalysis alignmentAnalysis) {
    Sequence query = querySection.getSequence();
    Sequence reference = referenceSection.getSequence();
    double maxInterestingPenalty = alignmentParameters.MaxErrorRate * query.getLength();

    // We want to determine a good size of blocks to split into
    // We don't want the blocks to be too small, or we won't be confident about the best offset in some blocks
    // We don't want the blocks to be too large, or the PathAligner will have to check even more positions in that block
    // Overall, we think it works well to split N hashblocks into sqrt(N) groups and form a block for each
    int numBasesToEncodeReferencePosition = (int)Math.log((double)referenceSection.getLength() / Math.log(4.0)) + 1;
    int numHashblocks = querySection.getLength() / numBasesToEncodeReferencePosition + 1;
    int targetNumHashblocksPerBlock = (int)Math.sqrt(numHashblocks) + 1;
    int targetBlockSize = targetNumHashblocksPerBlock * numBasesToEncodeReferencePosition;
    int numBlocks = querySection.getLength() / targetBlockSize;

    List<SequenceAlignment> result = new ArrayList<SequenceAlignment>(numBlocks);
    for (int i = 0; i < numBlocks; i++) {
      result.add(null);
    }
    double usedPenalty = 0;
    int numRemainingAlignments = numBlocks;
    while (true) {
      // whether there was any subalignment that we failed
      boolean failedSubalignment = false;
      // whether we failed to find one subalignment and then later found a different subalignment
      boolean failedSubalignmentThenFoundSubalignment = false;
      int startPosition = querySection.getStartIndex();
      // split the full match into smaller pieces
      for (int i = 0; i < numBlocks; i++) {
        int endPosition = querySection.getStartIndex() + (querySection.getLength() * (i + 1) / numBlocks);
        if (result.get(i) == null) {
          SequenceSection querySubsection = new SequenceSection(query, startPosition, endPosition);
          // align one piece
          double averagePenalty = (maxInterestingPenalty - usedPenalty) / numRemainingAlignments;
          SequenceAlignment subAlignment = this.alignPiece(querySubsection, referenceSection, averagePenalty, alignmentParameters, i == 0, alignmentAnalysis);
          if (subAlignment != null) {
            if (failedSubalignment) {
              failedSubalignmentThenFoundSubalignment = true;
            }
            numRemainingAlignments--;
            result.set(i, subAlignment);
            usedPenalty += subAlignment.getAlignedPenalty();
          } else {
            failedSubalignment = true;
          }
        }
        // repeat
        startPosition = endPosition;
      }
      if (numRemainingAlignments < 1)
        return result;
      if (!failedSubalignmentThenFoundSubalignment) {
        // Every time we find a subalignment, the penalty allocated to remaining subqueries might increase
        // If we never had a case where one subalignment failed a later subalignment succeeded, then we're not going to increase the penalty threshold for any of the failed subqueries, so we're done
        return null;
      }
    }
  }

  // merges these alignments into a smaller list
  private List<SequenceAlignment> joinAlignments(List<SequenceAlignment> alignments, SequenceSection referenceSection, AlignmentParameters alignmentParameters, double maxInterestingPenalty, AlignmentAnalysis alignmentAnalysis, boolean allowSimpleMerges) {
    /*if (this.logger.getEnabled()) {
      this.logger.log("BlockAligner trying to join " + alignments.size() + " alignments");
    }*/
    List<SequenceAlignment> result = new ArrayList<SequenceAlignment>((alignments.size() + 1) / 2);
    double usedPenalty = 0;
    for (int i = 0; i < alignments.size(); i++)
      usedPenalty += alignments.get(i).getAlignedPenalty();
    for (int i = 0; i < alignments.size(); i += 2) {
      SequenceAlignment merge;
      SequenceAlignment left = alignments.get(i);
      if (i + 1 < alignments.size()) {
        SequenceAlignment right = alignments.get(i + 1);
        merge = tryMerge(alignments.get(i), alignments.get(i + 1), alignmentParameters);
        if (merge == null) {
          // recompute this alignment
          usedPenalty -= left.getAlignedPenalty();
          usedPenalty -= right.getAlignedPenalty();
          SequenceSection querySubsection = new SequenceSection(left.getSequenceA(), left.getStartIndexA(), right.getEndIndexA());
          merge = alignPiece(querySubsection, referenceSection, maxInterestingPenalty - usedPenalty, alignmentParameters, i == 0, alignmentAnalysis);
          if (merge == null)
            return null; // no sufficient alignment exists
          usedPenalty += merge.getAlignedPenalty();
        } else {
          if (!allowSimpleMerges) {
            /*if (logger.getEnabled()) {
              logger.log("Skipping simple merge for now");
            }*/
            result.add(left);
            i--;
            //result.add(right);
            continue;
          }
        }
      } else {
        merge = left;
      }
      /*if (logger.getEnabled()) {
        logger.log("BlockAligner got combined result:");
        logger.log(" " + merge.getAlignedTextA() + " = query[" + merge.getStartIndexA() + ":" + merge.getEndIndexA() + "]");
        logger.log(" " + merge.getAlignedTextB() + " =   ref[" + merge.getStartIndexB() + ":" + merge.getEndIndexB() + "]");
      }*/
      result.add(merge);
    }
    return result;
  }

  private SequenceAlignment tryMerge(SequenceAlignment left, SequenceAlignment right, AlignmentParameters parameters) {
    SequenceAlignment result = doTryMerge(left, right, parameters);
    if (logger.getEnabled() && result == null) {
      logger.log("BlockAligner unable to merge alignents:");
      logger.log("  " + left.getAlignedTextA() + " = query[" + left.getStartIndexA() + ":" + left.getEndIndexA() + "]");
      logger.log("  " + left.getAlignedTextB() + " =   ref[" + left.getStartIndexB() + ":" + left.getEndIndexB() + "]");
      logger.log(" and");
      logger.log("  " + right.getAlignedTextA() + " = query[" + right.getStartIndexA() + ":" + right.getEndIndexA() + "]");
      logger.log("  " + right.getAlignedTextB() + " =   ref[" + right.getStartIndexB() + ":" + right.getEndIndexB() + "]");
    }
    return result;
  }
  private SequenceAlignment doTryMerge(SequenceAlignment left, SequenceAlignment right, AlignmentParameters parameters) {
    if (left.getEndIndexB() != right.getStartIndexB()) {
      if (logger.getEnabled()) {
        logger.log("BlockAligner cannot merge SequenceAlignment ending at " + left.getEndIndexB() + " with SequenceAlignment starting at " + right.getStartIndexB());
      }
    
      // the two alignment positions don't touch
      return null;
    }
    // merge the alignments
    List<AlignedBlock> leftSections = left.getSections();
    List<AlignedBlock> rightSections = right.getSections();
    AlignedBlock rightmostLeftSection = leftSections.get(leftSections.size() - 1);
    AlignedBlock leftmostRightSection = rightSections.get(0);
    AlignedBlock middleBlock = this.tryMergeBlocks(rightmostLeftSection, leftmostRightSection);
    if (middleBlock == null) {
      // neighbors disagree on adjacent indel type, so we have to recompute the alignment
      if (logger.getEnabled()) {
        logger.log("BlockAligner cannot merge SequenceAlignment ending at " + left.getEndIndexB() + " with SequenceAlignment starting at " + right.getStartIndexB());
      } 
      return null;
    }
    List<AlignedBlock> sections = new ArrayList<AlignedBlock>(leftSections.size() + rightSections.size());
    for (int i = 0; i < leftSections.size() - 1; i++) {
      sections.add(leftSections.get(i));
    }
    sections.add(middleBlock);
    for (int i = 1; i < rightSections.size(); i++) {
      sections.add(rightSections.get(i));
    }
    SequenceAlignment result = new SequenceAlignment(sections, parameters, left.isReferenceReversed());
    return result;
  }

  private AlignedBlock tryMergeBlocks(AlignedBlock left, AlignedBlock right) {
    if (!left.sameIndelType(right)) {
      if (logger.getEnabled()) {
        logger.log("cannot merge blocks " + left + " and " + right + " with different indel types");
      }
      return null;
    }
    if (left.getEndIndexA() != right.getStartIndexA()) {
      if (logger.getEnabled()) {
        logger.log("cannot merge blocks " + left + " ending at query index " + left.getEndIndexA() + " and " + right + " starting at query index " + right.getStartIndexA());
      }
      return null;
    }
    if (left.getEndIndexB() != right.getStartIndexB()) {
      if (logger.getEnabled()) {
        logger.log("cannot merge blocks " + left + " ending at reference index " + left.getEndIndexB() + " and " + right + " starting at reference index " + right.getStartIndexB());
      }
      return null;
    }
    return new AlignedBlock(left.getSequenceA(), left.getSequenceB(), left.getStartIndexA(), left.getStartIndexB(), left.getLengthA() + right.getLengthA(), left.getLengthB() + right.getLengthB());
  }

  // creates an alignment for the given match using another aligner
  private SequenceAlignment alignPiece(SequenceSection querySection, SequenceSection referenceSection, double maxPenalty, AlignmentParameters parameters, boolean firstPiece, AlignmentAnalysis parentAlignmentAnalysis) {
    if (maxPenalty < 0)
      return null;

    // get max total indel length and use that to put bounds on how much of the reference we need to consider
    SequenceSection referenceSubsection;
    if (parentAlignmentAnalysis.confidentAboutBestOffset) {
      int maxInsertionLength = (int)((double)parentAlignmentAnalysis.maxInsertionExtensionPenalty / (double)parameters.InsertionExtension_Penalty);
      int maxDeletionLength = (int)((double)parentAlignmentAnalysis.maxDeletionExtensionPenalty / (double)parameters.DeletionExtension_Penalty);
      int maxIndelLength = Math.max(maxInsertionLength, maxDeletionLength);

      int referenceStart = Math.max(referenceSection.getStartIndex(), querySection.getStartIndex() + parentAlignmentAnalysis.predictedBestOffset - maxIndelLength);
      int referenceEnd = Math.min(referenceSection.getEndIndex(), querySection.getEndIndex() + parentAlignmentAnalysis.predictedBestOffset + maxIndelLength);

      if (referenceEnd > referenceStart)
        referenceSubsection = new SequenceSection(referenceSection.getSequence(), referenceStart, referenceEnd);
      else
        referenceSubsection = referenceSection;
    } else {
      referenceSubsection = referenceSection;
    }

    AlignmentParameters subParameters = parameters.clone();
    if (!firstPiece) {
      subParameters.StartingInsertionStartFree = true;
    }
    subParameters.MaxErrorRate = maxPenalty / querySection.getLength();

    if (logger.getEnabled()) {
      logger.log("BlockAligner attempting to align query[" + querySection.getStartIndex() + ":" + querySection.getEndIndex() + "] and ref[" + referenceSubsection.getStartIndex() + ":" + referenceSubsection.getEndIndex() + "] with penalty <= " + maxPenalty);
    }
    AlignmentAnalysis childAnalysis = parentAlignmentAnalysis.child();
    childAnalysis.confidentAboutBestOffset = false;
    return this.nextAligner.align(querySection, referenceSubsection, subParameters, childAnalysis);
  }

  private Logger logger;
  private LocalAligner nextAligner;
}
