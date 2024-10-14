package mapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class QueryMatch_Aligner {
  public QueryMatch_Aligner(Query query, AlignmentParameters initialParameters, Logger logger) {
    this.logger = logger;
    this.verboseLogger = logger.incrementScope();
    this.parameters = initialParameters;
    this.query = query;
    this.aligner = buildAligner(this.verboseLogger.incrementScope());
  }

  private static LocalAligner buildAligner(Logger logger) {
    LocalAligner aligner = new PathAligner_Runner();
    aligner = new StraightAligner(aligner);
    aligner = new HashBlock_Aligner(aligner);
    aligner = new StraightAligner(aligner);
    aligner = new BlockAligner(aligner);
    aligner = new HashBlock_Aligner(aligner);
    aligner = new SkipHighAmbiguity_Aligner(aligner);
    aligner = new StraightAligner(aligner);
    aligner.setLogger(logger);
    return aligner;
  }

  public QueryAlignment align(QueryMatch match) {
    return this.align(match, 0);
  }

  public QueryAlignment align(QueryMatch match, double extraSpacing) {
    QueryAlignment alignment = this.doAlign(match, extraSpacing);
    if (alignment != null) {
      double targetPenalty = this.bestPenalty + this.parameters.Max_PenaltySpan;
      if (alignment.getPenalty() < this.bestPenalty) {
        // also reduce the minimum interesting error rate for this query
        this.bestPenalty = alignment.getPenalty();
        double newTargetPenalty = alignment.getPenalty() + parameters.Max_PenaltySpan;
        double newTargetErrorRate = divideRoundUp(newTargetPenalty, query.getLength());
        if (newTargetErrorRate < this.parameters.MaxErrorRate) {
          AlignmentParameters stricterParams = this.parameters.clone();
          stricterParams.MaxErrorRate = newTargetErrorRate;
          this.parameters = stricterParams;
        }
      }

      this.goodAlignments.add(alignment);
    }
    return alignment;
  }

  public double divideRoundUp(double a, double b) {
    double result = a / b;
    if (result * b < a)
      result = Math.nextUp(result);
    return result;
  }

  private QueryAlignment getExistingAlignment(QueryMatch queryMatch) {
    for (QueryAlignment candidate: this.goodAlignments) {
      if (candidate.containsSameOffsetAsMatch(queryMatch))
        return candidate;
    }
    return null;
  }

  public List<QueryAlignment> getBestAlignments() {
    double maxInterestingPenaltyAnywhere = query.getLength() * parameters.MaxErrorRate;
    double cutoffPenalty = this.bestPenalty + this.parameters.Max_PenaltySpan;
    if (cutoffPenalty > maxInterestingPenaltyAnywhere)
      cutoffPenalty = maxInterestingPenaltyAnywhere;

    List<QueryAlignment> bestAlignments = new ArrayList<QueryAlignment>(this.goodAlignments.size());
    for (QueryAlignment alignment: this.goodAlignments) {
      if (alignment.getPenalty() <= cutoffPenalty)
        bestAlignments.add(alignment);
    }
    return withoutDuplicates(bestAlignments);
  }

  // Checks for duplicate alignments in this list and returns a list without duplicates
  private List<QueryAlignment> withoutDuplicates(List<QueryAlignment> alignments) {
    if (alignments.size() <= 1) {
      return alignments;
    }
    Set<QueryAlignment> set = new HashSet<QueryAlignment>(alignments);
    return new ArrayList<QueryAlignment>(set);
  }

  private QueryAlignment doAlign(QueryMatch match, double extraSpacing) {
    double innerDistance = getSpacing(match) + extraSpacing;
    double spacingPenalty = computeSpacingPenalty(innerDistance);
    double overlapMultiplier = 1;
    double duplicationBonus = 0;
    double maxAllowedPenalty = match.getQueryTotalLength() * this.parameters.MaxErrorRate;
    // increase the limit slightly in case of previous rounding error
    maxAllowedPenalty = Math.nextUp(maxAllowedPenalty);
    if (this.logger.getEnabled()) {
      if (match.getNumSequences() > 1) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Trying to align query:");
        int i = 0;
        for (SequenceMatch sequenceMatch: match.getComponents()) {
          i++;
          if (i != 1)
            stringBuilder.append(" and");
          stringBuilder.append(" seq" + i + " at " + sequenceMatch.getSequenceB().getName() + " offset " + sequenceMatch.getOffset());
        }
        stringBuilder.append(" with combined penalty <= " + maxAllowedPenalty);
        stringBuilder.append(" (spacing penalty here = " + spacingPenalty + " for spacing " + match.getTotalDistanceBetweenComponents() + ")");
        logger.log(stringBuilder.toString());
      }
    }
    // Check for two query sequences so far apart that we can show that the spacing penalty plus mismatches is too much
    // If the query sequences overlap then we're not completely sure about the number of mismatches so we disable this check
    if (innerDistance > 0) {
      double minPossiblePenalty = spacingPenalty + match.getPriority() * this.parameters.MutationPenalty;
      if (minPossiblePenalty > maxAllowedPenalty) {
        if (this.logger.getEnabled()) {
          logger.log("min possible penalty = " + minPossiblePenalty + " for spacing penalty " + spacingPenalty + " and match priority " + match.getPriority() + " so skipping searching for alignments here");
        }
        return null;
      }
    }

    // Temporarily disabled because it doesn't always work correctly. Added a unit test.
    /*QueryAlignment existingAlignment = getExistingAlignment(match);
    if (existingAlignment != null) {
      if (logger.getEnabled()) {
        verboseLogger.log("Already have alignment at " + match.summarizePositionB());
      }
      return null;
    }*/

    List<SequenceAlignment> resultComponents = null;
    double componentsPenalty = 0; // total penalty of the individual sequences
    if (match.getNumSequences() > 1 && innerDistance < 0) {
      // check for overlapping paired-end queries
      if (verboseLogger.getEnabled())
        verboseLogger.log("Trying to join query sequences with inner distance = " + innerDistance);
      Sequence joined = tryJoinQuerySequences(match);
      if (joined != null) {
        SequenceAlignment joinedAlignment = this.computeJoinedAlignment(joined, match);
        if (verboseLogger.getEnabled()) {
          if (joinedAlignment != null) {
            verboseLogger.log("Got joined alignment");
          } else {
            verboseLogger.log("Got joined alignment = null");
          }
        }
        resultComponents = this.splitAlignment(joinedAlignment, match);
        if (resultComponents == null)
          return null;
        for (SequenceAlignment component: resultComponents) {
          componentsPenalty += component.getPenalty();
        }
      }
    }
    if (resultComponents == null) {
      // We didn't have paired-end queries or they didn't overlap or we couldn't join them
      // Here we just do a normal alignment
      resultComponents = new ArrayList<SequenceAlignment>(match.getNumSequences());
      List<SequenceMatch> remainingQueryComponents = new ArrayList<SequenceMatch>(match.getComponents());
      for (int i = 0; i < match.getComponents().size(); i++) {
        resultComponents.add(null);
      }
      int numRemainingComponents = match.getComponents().size();

      boolean checkComponentsInForwardOrder = match.get_hintCheckComponentsInForwardOrder();
      int firstComponentIndex;
      int componentIndexStep;
      int lastComponentIndex;
      if (checkComponentsInForwardOrder) {
        firstComponentIndex = 0;
        componentIndexStep = 1;
        lastComponentIndex = match.getNumSequences();
      } else {
        firstComponentIndex = match.getNumSequences() - 1;
        componentIndexStep = -1;
        lastComponentIndex = -1;
      }

      double maxTotalComponentPenalty;
      if (innerDistance < 0 && match.getNumSequences() > 1) {
        // If query sequences overlap, it's possible that all of the mutations are in the overlapping section
        // If all of the mutations are in the overlapping section, the sum of the penalties of the components could be double the total penalty
        double queryTotalLength = match.getQueryTotalLength();
        double estimatedOverlap = Math.min(-1 * innerDistance, Math.min(match.getComponent(0).getSequenceA().getLength(), match.getComponent(1).getSequenceA().getLength()));
        double estimatedUniqueLength = queryTotalLength - estimatedOverlap;

        // reported penalty = (component1.penalty + component2.penalty - duplicatedPenalty) * (totalLength / uniqueLength) + spacingPenalty
        // (reported penalty - spacingPenalty) = (component1.penalty + component2.penalty - duplicatedPenalty) * (totalLength / uniqueLength)
        // (reported penalty - spacingPenalty) * (uniqueLength / totalLength) = component1.penalty + component2.penalty - duplicatedPenalty
        // It's possible that duplicated penalty = (component1.penalty + component2.penalty) / 2
        // (maxTotalComponentPenalty - spacingPenalty) * (uniqueLength / totalLength) = totalComponentPenalty / 2
        maxTotalComponentPenalty = divideRoundUp(maxAllowedPenalty - spacingPenalty, queryTotalLength) * estimatedUniqueLength * 2;
        if (verboseLogger.getEnabled()) {
          verboseLogger.log("max allowed total penalty for both query sequences = " + maxTotalComponentPenalty + " = (maxAllowedPenalty (" + maxAllowedPenalty + ") - spacingPenalty (" + spacingPenalty + ")) / queryTotalLength (" + queryTotalLength + ") * uniqueLength (" + estimatedUniqueLength + ")");
        }
      } else {
        maxTotalComponentPenalty = maxAllowedPenalty - spacingPenalty;
      }
      while (true) {
        // determine average penalty (per base pair) allowed for unaligned sequences
        int numBases = this.countQueryLength(remainingQueryComponents);
        if (numBases < 1)
          break;
        double averagePenaltyPerRemainingItem = divideRoundUp(maxTotalComponentPenalty - componentsPenalty, numBases);
        AlignmentParameters parametersForRemainingSequences = this.parameters.clone();
        parametersForRemainingSequences.MaxErrorRate = averagePenaltyPerRemainingItem;
        // Check for any sequence having an alignment with penalty at least as good as the required average
        boolean foundAMatch = false;

        for (int i = firstComponentIndex; i != lastComponentIndex; i += componentIndexStep) {
          SequenceMatch componentMatch = remainingQueryComponents.get(i);
          if (componentMatch != null) {
            SequenceAlignment sequenceAlignment = this.alignMatch(componentMatch, parametersForRemainingSequences);
            if (sequenceAlignment != null) {
              resultComponents.set(i, sequenceAlignment);
              foundAMatch = true;
              remainingQueryComponents.set(i, null);
              componentsPenalty += sequenceAlignment.getPenalty();
              numRemainingComponents--;
              break; // recompute penalty values before checking for alignments in other sequences
            }
          }
        }
        if (numRemainingComponents < 1) {
          break;
        }
        if (!foundAMatch) {
          // not making any progress, so we're done
          return null;
        }
      }
    }
    double totalUsedPenalty = componentsPenalty;
    if (innerDistance < 0) {
      duplicationBonus = computeDuplicationBonus(resultComponents);
      totalUsedPenalty -= duplicationBonus;
      double multipliedPenalty = multiplyPenaltyForOverlap(resultComponents, totalUsedPenalty);

      if (totalUsedPenalty != 0) {
        overlapMultiplier = multipliedPenalty / totalUsedPenalty;
      } else {
        overlapMultiplier = 1;
      }
      totalUsedPenalty = multipliedPenalty;
    }
    totalUsedPenalty += spacingPenalty;
    if (totalUsedPenalty > maxAllowedPenalty) {
      if (verboseLogger.getEnabled()) {
        verboseLogger.log("sequences penalty = " + componentsPenalty + ", duplication bonus = " + duplicationBonus + ", overlap multiplier = " + overlapMultiplier + ", spacing penalty = " + spacingPenalty + ", total penalty = " + totalUsedPenalty + " > max allowed penalty " + maxAllowedPenalty);
      }
      return null;
    }
    int actualInnerDistance;
    if (resultComponents.size() > 1) {
      actualInnerDistance = resultComponents.get(1).getStartIndexB() - resultComponents.get(0).getEndIndexB();
    } else {
      actualInnerDistance = 0;
    }
    QueryAlignment result = new QueryAlignment(resultComponents, spacingPenalty, overlapMultiplier, duplicationBonus, totalUsedPenalty, actualInnerDistance);
    if (verboseLogger.getEnabled() && match.getComponents().size() > 1) {
      verboseLogger.log("Query alignment penalty = " + totalUsedPenalty + ": " + result.explainPenalty() + " <= max interesting penalty " + maxAllowedPenalty);
    }
    return result;
  }

  private Sequence tryJoinQuerySequences(QueryMatch match) {
    SequenceMatch match1 = match.getComponent(0);
    Sequence sequence1 = match1.getSequenceA();
    SequenceMatch match2 = match.getComponent(1);
    Sequence sequence2 = match2.getSequenceA();
    int offset = match2.getOffset() - match1.getOffset();
    if (offset >= 0)
      return tryJoinQuerySequences(sequence1, sequence2, offset);
    else
      return tryJoinQuerySequences(sequence2, sequence1, -offset);
  }

  // offset encodes the expected position of match2 in match1
  private Sequence tryJoinQuerySequences(Sequence sequence1, Sequence sequence2, int offset) {
    // confirm that the sequences overlap
    int suffixStartIndex = sequence1.getLength() - offset;
    if (suffixStartIndex < 0) {
      if (verboseLogger.getEnabled()) {
        verboseLogger.log("Cannot join sequence matches: sequence1 length (" + sequence1.getLength() + ") < offset (" + offset + ")");
      }
      return null;
    }

    int match2IndexEnd = Math.min(sequence2.getLength(), sequence1.getLength() - offset); // match2Index < sequence2.length; match2Index - offset < sequence1.length
    for (int match2Index = 0; match2Index < match2IndexEnd; match2Index++) {
      int match1Index = match2Index + offset;
      byte encodedChar1 = sequence1.encodedCharAt(match1Index);
      byte encodedChar2 = sequence2.encodedCharAt(match2Index);
      if (encodedChar1 != encodedChar2) {
        if (verboseLogger.getEnabled())
          verboseLogger.log("Cannot join sequence matches: sequence1[" + match1Index + "](" + Basepairs.decode(encodedChar1) + ") != sequence2[" + match2Index + "](" + Basepairs.decode(encodedChar2) + ")");
        return null;
      }
    }
    // now join the sequences at this offset
    SequenceBuilder joinedBuilder = new SequenceBuilder().setName("joined");
    String prefix = sequence1.getText();
    int endIndex = sequence2.getLength();
    String suffix = sequence2.getRange(suffixStartIndex, endIndex - suffixStartIndex);
    joinedBuilder.add(prefix);
    joinedBuilder.add(suffix);
    Sequence joined = joinedBuilder.build();
    if (verboseLogger.getEnabled())
      verboseLogger.log("Joined query sequences into query of length " + joined.getLength() + ": " + joined.getText());
    return joined;
  }

  private SequenceAlignment computeJoinedAlignment(Sequence joined, QueryMatch originalMatch) {
    int joinedOffset = Math.min(originalMatch.getComponent(0).getOffset(), originalMatch.getComponent(1).getOffset());
    SequenceMatch joinedMatch = new SequenceMatch(joined, originalMatch.getComponent(0).getSequenceB(), joinedOffset);

    // Slightly increase the threshold for this step in case of rounding error
    // We will still re-check the total penalty later anyway
    AlignmentParameters subParameters = this.parameters.clone();
    subParameters.MaxErrorRate = Math.nextUp(subParameters.MaxErrorRate);
    return alignMatch(joinedMatch, subParameters);
  }

  private List<SequenceAlignment> splitAlignment(SequenceAlignment joinedAlignment, QueryMatch queryMatch) {
    if (joinedAlignment == null)
      return null;
    SequenceMatch match1 = queryMatch.getComponent(0);
    Sequence sequence1 = match1.getSequenceA();
    SequenceMatch match2 = queryMatch.getComponent(1);
    Sequence sequence2 = match2.getSequenceA();

    int offset = match2.getOffset() - match1.getOffset();
    SequenceAlignment alignment1;
    SequenceAlignment alignment2;
    if (offset >= 0) {
      alignment1 = extract(joinedAlignment, 0, sequence1.getLength(), sequence1, match1.getReversed());
      alignment2 = extract(joinedAlignment, offset, sequence2.getLength() + offset, sequence2, match2.getReversed());
    } else {
      alignment2 = extract(joinedAlignment, 0, sequence2.getLength(), sequence2, match2.getReversed());
      alignment1 = extract(joinedAlignment, -offset, sequence1.getLength() - offset, sequence1, match1.getReversed());
    }
    if (alignment1 == null || alignment2 == null) {
      // If the joined alignment falls off of the end of the reference so much that one query sequence is unused, then we shouldn't count this as a paired-end alignment
      // Instead we separately check for other paired-end alignments or potentially other unpaired alignments near the ends of reference contigs
      return null;
    }
    List<SequenceAlignment> alignments = new ArrayList<SequenceAlignment>(2);
    alignments.add(alignment1);
    alignments.add(alignment2);

    return alignments;
  }

  private SequenceAlignment extract(SequenceAlignment joinedAlignment, int queryStart, int queryEnd, Sequence query, boolean reverse) {
    boolean referenceReversed = joinedAlignment.isReferenceReversed() != reverse;
    if (verboseLogger.getEnabled())
      verboseLogger.log("Extracting alignment positions " + queryStart + "-" + queryEnd);

    Sequence reference = joinedAlignment.getSequenceB();
    List<AlignedBlock> blocks = new ArrayList<AlignedBlock>();
    for (AlignedBlock block: joinedAlignment.getSections()) {
      if (block.getStartIndexA() >= queryEnd)
        break;
      if (block.getEndIndexA() <= queryStart)
        continue;
      int selectionStart = Math.max(block.getStartIndexA(), queryStart);
      int selectionEnd = Math.min(block.getEndIndexA(), queryEnd);
      int querySelectionLength = selectionEnd - selectionStart;
      int referenceSelectionLength;
      int referenceStart;
      if (block.getLengthA() == block.getLengthB()) {
        // 1-1 alignment
        referenceSelectionLength = querySelectionLength;
        referenceStart = selectionStart + block.getOffset();
      } else {
        if (block.getLengthA() > block.getLengthB()) {
          // insertion
          referenceSelectionLength = 0;
          referenceStart = block.getStartIndexB();
        } else {
          // deletion
          referenceSelectionLength = block.getLengthB();
          referenceStart = selectionStart + block.getOffset();
        }
      }
      blocks.add(new AlignedBlock(query, reference, selectionStart - queryStart, referenceStart, querySelectionLength, referenceSelectionLength));
    }
    if (blocks.size() < 1) {
      if (verboseLogger.getEnabled())
        verboseLogger.log("Extracting query positions " + queryStart + " to " + queryEnd + " found no aligned blocks");
      return null;
    }
    SequenceAlignment result = new SequenceAlignment(blocks, this.parameters, referenceReversed);
    if (verboseLogger.getEnabled())
      verboseLogger.log("Extracted alignment with penalty " + result.getPenalty() + ":\n " + result.getAlignedTextA() + "\n " + result.getAlignedTextB());
    return result;
  }

  private double computeUngappedPenalty(SequenceMatch sequenceMatch) {
    AlignedBlock block = new AlignedBlock(sequenceMatch.getSequenceA(), sequenceMatch.getSequenceB(), sequenceMatch.getStartIndexA(), sequenceMatch.getStartIndexB(), sequenceMatch.getLength(), sequenceMatch.getLength());
    return block.getPenalty(this.parameters);
  }

  private SequenceAlignment alignMatch(SequenceMatch sequenceMatch, AlignmentParameters parameters) {
    SequenceSection querySection = new SequenceSection(sequenceMatch.getSequenceA(), sequenceMatch.getStartIndexA(), sequenceMatch.getEndIndexA());
    double maxInterestingPenalty = querySection.getLength() * parameters.MaxErrorRate;
    int maxIndelLength = (int)Math.max((double)0, (double)(maxInterestingPenalty - parameters.DeletionStart_Penalty) / parameters.DeletionExtension_Penalty);
    int maxShift;
    int bestOffset = sequenceMatch.getOffset();
    if (sequenceMatch.fromHashblockMatch) {
      maxShift = maxIndelLength;
    } else {
      // If this match was generated by a guess then we have to search more area for possible alignments
      maxShift = (int)((double)maxInterestingPenalty * (double)this.query.getSpacingDeviationPerUnitPenalty());
      if (maxShift < 0)
        return null;
      if (bestOffset + sequenceMatch.getSequenceA().getLength() > sequenceMatch.getSequenceB().getLength())
        bestOffset = sequenceMatch.getSequenceB().getLength() - sequenceMatch.getSequenceA().getLength();
      if (bestOffset < 0)
        bestOffset = 0;
      querySection = new SequenceSection(sequenceMatch.getSequenceA(), 0, sequenceMatch.getSequenceA().getLength());
    }

    SequenceSection referenceSection = new SequenceSection(sequenceMatch.getSequenceB(), Math.max(0, sequenceMatch.getStartIndexB() - maxShift), Math.min(sequenceMatch.getEndIndexB() + maxShift, sequenceMatch.getSequenceB().getLength()));

    AlignmentAnalysis alignmentAnalysis = new AlignmentAnalysis();
    alignmentAnalysis.maxInsertionExtensionPenalty = maxInterestingPenalty - parameters.InsertionStart_Penalty;
    alignmentAnalysis.maxDeletionExtensionPenalty = maxInterestingPenalty - parameters.DeletionStart_Penalty;
    alignmentAnalysis.predictedBestOffset = bestOffset;
    alignmentAnalysis.confidentAboutBestOffset = sequenceMatch.fromHashblockMatch;

    if (verboseLogger.getEnabled()) {
      double maxAllowedPenalty = querySection.getSequence().getLength() * parameters.MaxErrorRate + parameters.Max_PenaltySpan;
      verboseLogger.log("Trying to align " + sequenceMatch.summarize() + " having penalty <= " + maxAllowedPenalty);
    }
    SequenceAlignment result = aligner.align(querySection, referenceSection, parameters, alignmentAnalysis);
    if (verboseLogger.getEnabled()) {
      String prefix = "Alignment result for " + sequenceMatch.summarize();
      String suffix;
      if (result != null) {
        String offsetExplanation;
        int alignmentOffset = result.getStartOffset();
        if (alignmentOffset != sequenceMatch.getOffset())
          offsetExplanation = "offset " + alignmentOffset + " and ";
        else
          offsetExplanation = "";
        suffix = " has " + offsetExplanation + "penalty " + result.getPenalty() + ":\n " + result.format().replace("\n", "\n ");
      } else {
        suffix = " = null";
      }
      verboseLogger.log(prefix + suffix);
    }
    return result;
  }

  private double multiplyPenaltyForOverlap(List<SequenceAlignment> components, double totalPenalty) {
    if (components.size() < 2)
      return totalPenalty;
    SequenceAlignment first = components.get(0);
    SequenceAlignment second = components.get(1);

    double overlappingLengthB = Math.min(first.getEndIndexB(), second.getEndIndexB()) - Math.max(first.getStartIndexB(), second.getStartIndexB());
    if (overlappingLengthB <= 0) {
      // If the alignments don't overlap, we don't need to adjust the penalty
      return totalPenalty;
    }

    // Compute unique length of sequence a
    int uniqueLengthA;
    if (first.getStartIndexB() <= second.getStartIndexB()) {
      uniqueLengthA = first.getLengthABefore(second.getStartIndexB()) + second.getLengthA() + first.getLengthAAfter(second.getEndIndexB());
    } else {
      uniqueLengthA = second.getLengthABefore(first.getStartIndexB()) + first.getLengthA() + second.getLengthAAfter(first.getEndIndexB());
    }
    // Also subtract any deletions from the overlapping length because this makes it easier for us to estimate an upper bound on this number without actually checking for indels
    double deletion = Math.min(first.getInsertAOrBLength(), second.getInsertAOrBLength());
    uniqueLengthA -= deletion;
    if (deletion > 0) {
      if (verboseLogger.getEnabled())
        verboseLogger.log("Decreased unique length A by deletion length " + deletion + " to " + uniqueLengthA + " to make it easier for us to compute an upper bound");
    }
    if (uniqueLengthA <= 0) {
      return totalPenalty;
    }

    int totalLengthA = first.getLengthA() + second.getLengthA();

    // If the query sequences overlap, we don't want to double-count any mutations in the overlapping section.
    // We want the penalty to be essentially the same as if they were joined into one sequence.
    // It would be nice to rescale the penalty threshold based on the shorter total length, but that would be slightly more complicated to compare across candidate alignments.
    // Instead, we rescale the penalty of each component
    double multipliedPenalty = divideRoundUp(totalPenalty, uniqueLengthA) * totalLengthA;
    if (verboseLogger.getEnabled())
      verboseLogger.log("multipliedPenalty = " + multipliedPenalty + " = totalPenalty (" + totalPenalty + ") / uniqueLengthA (" + uniqueLengthA + ") * totalLengthA (" + totalLengthA + ")");
    return multipliedPenalty;
  }

  private double computeDuplicationBonus(List<SequenceAlignment> components) {
    if (components.size() < 2)
      return 0;
    SequenceAlignment a = components.get(0);
    SequenceAlignment b = components.get(1);

    double overlappingLength = Math.min(a.getEndIndexB(), b.getEndIndexB()) - Math.max(a.getStartIndexB(), b.getStartIndexB());
    if (overlappingLength < 0) {
      // If the alignments don't overlap, we don't need to adjust the penalty
      return 0;
    }
    // Any mismatches in the overlapping section currently get counted twice but we don't want them to be
    double duplicatedPenalty = (a.getPenalty(this.parameters, b.getStartIndexB(), b.getEndIndexB()) + b.getPenalty(this.parameters, a.getStartIndexB(), a.getEndIndexB())) / 2;
    return duplicatedPenalty;
  }

  private int getSpacing(QueryMatch match) {
    if (match.getNumSequences() < 2)
      return 0;
    return match.getTotalDistanceBetweenComponents();
  }

  // This function determines how surprising it is for us to observe this distance between the sequence alignments in a query, and converts that surprise into a penalty
  // More surprising distances are assigned more penalty
  private double computeSpacingPenalty(double innerDistance) {
    double expected = query.getExpectedInnerDistance();
    int totalLength = query.getLength();
    if (innerDistance < 0 && innerDistance > -1 * totalLength) {
      // The query sequences overlap
      // If we get here then the query sequences are probably similar
      // We could've previously computed that the query sequences are similar, but it probably wouldn't have been very helpful to compute, because it's supposed to be unlikely, and also because it shouldn't be much more effort to just do the normal alignment process anyway

      // If the query sequences are similar (which we might have been able to compute before doing the alignment but we didn't), then it's not surprising for the query sequence alignments to overlap

      // If it's not surprising for the query sequence alignments to overlap, then the spacing penalty is 0
      return 0;
    }
    double deviationPerPenalty = query.getSpacingDeviationPerUnitPenalty();
    int penalty = (int)(Math.abs(innerDistance - expected) / deviationPerPenalty);
    return (double)penalty;
  }

  private int countQueryLength(List<SequenceMatch> components) {
    int total = 0;
    for (SequenceMatch match: components) {
      if (match != null)
        total += match.getSequenceA().getLength();
    }
    return total;
  }

  private AlignmentParameters parameters;
  private Logger logger;
  private Logger verboseLogger;
  private int blockLength;
  private LocalAligner aligner;
  private Query query;
  private List<QueryAlignment> goodAlignments = new ArrayList<QueryAlignment>();
  private double bestPenalty = Integer.MAX_VALUE;
}
