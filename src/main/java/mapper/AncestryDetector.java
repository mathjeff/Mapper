package mapper;

import java.io.FileNotFoundException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

// An AncestryDetector tries to estimate the ancestry of certain positions in a genome
// It uses information from similar sections of the genome
public class AncestryDetector implements ReferenceProvider {
  public AncestryDetector(DuplicationDetector duplicationDetector, List<Sequence> reference, double dissimilarityThreshold, StatusLogger statusLogger) {
    this.statusLogger = statusLogger;
    this.duplicationDetector = duplicationDetector;
    this.reference = reference;
    this.sequenceOverrides = new LinkedHashMap<Sequence, OverriddenSequence>();
    this.sequencesByOverride = new HashMap<Sequence, Sequence>();
    this.dissimilarityThreshold = dissimilarityThreshold;
    for (Sequence sequence: reference) {
      OverriddenSequence newSequence = new OverriddenSequence(sequence, sequence.getName() + "-anc");
      sequenceOverrides.put(sequence, newSequence);
      sequencesByOverride.put(newSequence, sequence);
    }
  }

  public void setVerifyNoDuplicateAnalyses() {
    this.verifyNoDuplicateAnalyses = true;
  }

  public AncestryDetector setOutputPath(String outputPath) throws FileNotFoundException {
    if (outputPath != null) {
      this.outputWriter = new FastaWriter(outputPath);
    }
    return this;
  }

  public AncestryDetector setResultingDatabaseEnableGapmers(boolean enable) {
    this.resultingDatabaseEnableGapmers = enable;
    return this;
  }

  public boolean getCanUseHelp() {
    HashBlock_Database hashblockDatabase;
    synchronized(this) {
      hashblockDatabase = this.result;
    }
    return hashblockDatabase == null || hashblockDatabase.getCanUseHelp();
  }

  public HashBlock_Database get_HashBlock_database(Logger logger) {
    return unionRecentAncestors(logger);
  }

  public Sequence getOriginalSequence(Sequence modified) {
    return this.sequencesByOverride.get(modified);
  }

  private void considerSavingDatabase(SequenceDatabase sequenceDatabase) {
    if (this.outputWriter != null) {
      for (Sequence sequence: sequenceDatabase.getAll()) {
        if (sequence.getComplementedFrom() == null)
          outputWriter.write(sequence);
      }
      outputWriter.close();
    }
  }

  // Returns a new SequenceDatabase where for each position for which we think we know the common ancestor, that position is modified to be ambiguous, specifying that it can either be its previous value or its ancestor
  public HashBlock_Database unionRecentAncestors(Logger logger) {
    // Check whether we're already done
    synchronized(this) {
      if (this.result != null)
        return this.result;
      this.numActiveWorkers++;
    }

    // Process duplications one at a time
    Readable_DuplicationDetector duplicationDetector = this.duplicationDetector.getView(logger);
    while(true) {
      Duplication duplication = this.getNextDuplicationToProcess(duplicationDetector, logger);
      if (duplication == null) {
        break;
      }
      // process this duplication
      this.analyze(duplication, duplicationDetector, logger);
    }
    // the last worker should put the results together
    synchronized(this) {
      this.numActiveWorkers--;
      if (this.numActiveWorkers < 1) {
        // Make a SequenceDatabase having essentially our overridden sequences

        // First we discard the reverse sequences and regenerate them from the forward overrides so the reverse sequences know which forward overrides they were complemented from
        List<Sequence> forwardOverrides = new ArrayList<Sequence>();
        for (Map.Entry<Sequence, OverriddenSequence> entry: this.sequenceOverrides.entrySet()) {
          Sequence original = entry.getKey();
          Sequence overridden = entry.getValue();
          if (original.getComplementedFrom() == null) {
            // Also decompress these sequences because OverriddenSequence can otherwise be a bit slow
            overridden.decompress();
            forwardOverrides.add(overridden);
          }
        }
        SequenceDatabase sequenceDatabase = new SequenceDatabase(forwardOverrides, true);
        this.result = new HashBlock_Database(sequenceDatabase, -1, -1, -1, this.resultingDatabaseEnableGapmers, this.statusLogger);
        this.considerSavingDatabase(sequenceDatabase);
        System.err.println("AncestryDetector done");
      }
    }
    // wait for all workers to be done
    while(true) {
      synchronized(this) {
        if (this.result != null) {
          break;
        }
      }
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
      }
    }
    return this.result;
  }

  public boolean getEnableGapmers() {
    return this.resultingDatabaseEnableGapmers;
  }

  private void analyze(Duplication duplication, Readable_DuplicationDetector duplicationDetector, Logger logger) {
    this.analyze(duplication, duplicationDetector, -1, logger);
    this.analyze(duplication, duplicationDetector, 1, logger);
  }

  private void analyze(Duplication duplication, Readable_DuplicationDetector duplicationDetector, int polarity, Logger logger) {
    if (duplication.getNumInstances() < 3) {
      // not enough copies for us to use it
      return;
    }
    // compute bounds
    // availableComponents is the set of SimilarityAnalysis that we can use to help us infer common ancestors
    Set<SimilarityAnalysis> availableComponents = new HashSet<SimilarityAnalysis>();

    // interestedComponents is the set of SimilarityAnalysis for which we're trying to infer the common ancestor
    Set<SimilarityAnalysis> interestedComponents = new HashSet<SimilarityAnalysis>();

    for (SequencePosition startPosition: duplication.getStartPositions()) {
      SimilarityAnalysis analysis = computeAnalysisBounds(duplication, startPosition, duplicationDetector, polarity);
      if (analysis != null) {
        availableComponents.add(analysis);
        TreeMap<Integer, Duplication> interestingPositions = duplicationDetector.getInterestingDuplicationsOnSequence(startPosition.getSequence());
        Duplication interestingDuplicationHere = interestingPositions.get(startPosition.getStartIndex());
        if (duplication == interestingDuplicationHere)
          interestedComponents.add(analysis);
      }
    }
    if (logger.getEnabled()) {
      StringBuilder messageBuilder = new StringBuilder();
      messageBuilder.append("For " + duplication + " polarity " + polarity + ", created " + availableComponents.size() + " similarity analyses: ");
      for (SimilarityAnalysis analysis: availableComponents) {
        messageBuilder.append(analysis.toString());
        messageBuilder.append(", ");
      }
      logger.log(messageBuilder.toString());
    }
    // analyze
    // for each offset, we keep track of how many of each allele we found there
    List<Byte> mostPopularEncodedAlleles = new ArrayList<Byte>();
    byte noAncestor = Basepairs.encode('-');
    while (interestedComponents.size() >= 1 && availableComponents.size() >= 3) {
      Set<SimilarityAnalysis> noLongerInterestedComponents = new HashSet<SimilarityAnalysis>();
      Set<SimilarityAnalysis> noLongerAvailableComponents = new HashSet<SimilarityAnalysis>();

      // check for analyses that no longer need ancestor inferences
      Map<Byte, Integer> countsHere = new HashMap<Byte, Integer>();
      for (SimilarityAnalysis similarity: interestedComponents) {
        int currentPosition = similarity.currentIndex;
        if (similarity.currentIndex == similarity.boundIndex) {
          if (interestedComponents.contains(similarity)) {
            if (logger.getEnabled())
              logger.log("No longer interested in " + similarity + " (offset = " + mostPopularEncodedAlleles.size() + ") because reached bound " + similarity.boundIndex);
            noLongerInterestedComponents.add(similarity);
          }
        }
      }
      // count the distribution of alleles at this next offset
      for (SimilarityAnalysis similarity: availableComponents) {
        int currentPosition = similarity.currentIndex;
        Sequence sequence = similarity.sequence;
        if (currentPosition < 0 || currentPosition >= sequence.getLength()) {
          noLongerAvailableComponents.add(similarity);
          if (interestedComponents.contains(similarity)) {
            noLongerInterestedComponents.add(similarity);
            if (logger.getEnabled())
              logger.log("No longer interested in " + similarity + " because reached end " + currentPosition);
          }
        } else {
          byte itemHere = sequence.encodedCharAt(currentPosition);
          Integer existingCount = countsHere.get(itemHere);
          if (existingCount == null) {
            existingCount = 0;
          }
          countsHere.put(itemHere, existingCount + 1);
        }
      }

      // find the most popular allele at this offset
      int bestCount = 0;
      byte mostPopularEncodedItem = 0;
      boolean tie = false;
      for (Map.Entry<Byte, Integer> entry: countsHere.entrySet()) {
        byte item = entry.getKey();
        int count = entry.getValue();
        if (count > bestCount) {
          bestCount = count;
          mostPopularEncodedItem = item;
          tie = false;
        } else {
          if (count == bestCount) {
            tie = true;
          }
        }
      }
      if (tie) {
        if (logger.getEnabled())
          logger.log("For " + duplication + " offset = " + mostPopularEncodedAlleles.size() + " most popular allele is tied (num readable sections here = " + availableComponents.size() + ", highest allele count = " + bestCount + ")");
        //noLongerInterestedComponents = new HashSet<SimilarityAnalysis>(interestedComponents);
        mostPopularEncodedItem = noAncestor;
      }
      mostPopularEncodedAlleles.add(mostPopularEncodedItem);

      // remove any similarity analyses that reached the end of the region for which they're responsible for inferring ancestors for
      for (SimilarityAnalysis similarity: noLongerInterestedComponents) {
        boolean hasNeighborAnalysis = !similarity.getReachedEndOfSequence();
        boolean hasScore = similarity.cumulativeScore >= 0;
        boolean reachedNeighborAnalysis = hasNeighborAnalysis && hasScore;
        if (reachedNeighborAnalysis) {
          // This similarity analysis reached another similarity analysis
          // If we break this similarity analysis earlier than at the neighbor, this means:
          //  We are splitting the similarity into two
          //  We are assigning any subsequent mutations to a non-similar section rather than to a similar section
          // The probability of observing a new similar section is small, smaller than the probability of observing a SNP
          //  Suppose that the probability of observing a new similar section is at least as low as the probability of observing two SNPs
          // The probability (of observing a SNP in a specific position of a non-similar section of the reference) minus the probability of (observing a SNP in a specific position of a specific section of a similar section of the reference) is less than the probability of (observing a SNP in a specific position in the reference)
          //  Suppose that this change in probability is less than two thirds the probability of observing a SNP
          // Then, the probability of observing a new similar section is at least as low as the probability of moving three SNPs from a non-similar section to a similar section
          // So, we consider it likely that similar section will extend to its neighbor even if there are three SNPs between the highest-scoring position and the neighbor
          similarity.addScore(getMismatchScore(3) * -1);
          if (logger.getEnabled()) {
            logger.log("For " + duplication + " offset = " + mostPopularEncodedAlleles.size() + " applying bonus score for similarity that reached the end " + similarity + ": new score = " + similarity.cumulativeScore);
          }
        }
        interestedComponents.remove(similarity);
      }

      // remove any similarity analyses that reached the end
      for (SimilarityAnalysis similarity: noLongerAvailableComponents) {
        availableComponents.remove(similarity);
      }

      // update score of similarity analyses
      for (SimilarityAnalysis similarity: availableComponents) {
        byte encodedItemHere = similarity.sequence.encodedCharAt(similarity.currentIndex);
        double scoreHere;
        if (encodedItemHere == mostPopularEncodedItem) {
          scoreHere = getMatchScore(1);
        } else {
          scoreHere = getMismatchScore(1);
        }
        similarity.addScore(scoreHere);

        if (similarity.cumulativeScore < 0) {
          noLongerAvailableComponents.add(similarity);
          if (interestedComponents.contains(similarity))
            noLongerInterestedComponents.add(similarity);
          if (logger.getEnabled())
            logger.log("No longer considering " + similarity + " (offset = " + mostPopularEncodedAlleles.size() + ") because cumulativeScore = " + similarity.cumulativeScore);
        } else {
          //if (logger.getEnabled()) {
          //  logger.log("Similarity " + similarity + " offset " + mostPopularEncodedAlleles.size() + " cumulativeScore = " + similarity.cumulativeScore + " bestScore = " + similarity.bestScore);
          //}
        }
      }

      // remove any similarity analyses we're no longer interested in
      for (SimilarityAnalysis similarity: noLongerAvailableComponents) {
        availableComponents.remove(similarity);
      }
      // remove any similarity analyses whose score is insufficient
      for (SimilarityAnalysis similarity: noLongerInterestedComponents) {
        interestedComponents.remove(similarity);
      }
      // advance to next position
      for (SimilarityAnalysis similarity: availableComponents) {
        similarity.currentIndex += polarity;
      }

      // write differences
      for (SimilarityAnalysis similarity: noLongerInterestedComponents) {
        for (int offset = 0; offset < mostPopularEncodedAlleles.size(); offset++) {
          int index = similarity.startIndex + offset * polarity;
          if (index == similarity.boundIndex) {
            if (logger.getEnabled())
              logger.log("For duplication " + duplication + " similarity " + similarity + " done inferring ancestors at offset " + offset + " because reached bound index " + index);
            break;
          }
          byte encodedCommonAncestor = mostPopularEncodedAlleles.get(offset);
          byte itemHere = similarity.sequence.encodedCharAt(index);
          if ((encodedCommonAncestor != itemHere && encodedCommonAncestor != noAncestor) || (this.verifyNoDuplicateAnalyses)) {
            byte hereOrAncestor = Basepairs.union(encodedCommonAncestor, itemHere);
            if (logger.getEnabled())
              logger.log("For duplication " + duplication + ", similarity " + similarity + " inferred ancestor " + Basepairs.decode(encodedCommonAncestor) + " (here = " + Basepairs.decode(itemHere) + ", union = " + Basepairs.decode(hereOrAncestor) + ") index " + index + " (offset " + offset + ")");
            this.write(similarity.sequence, index, Basepairs.union(encodedCommonAncestor, itemHere), similarity);
          }
          if (index == similarity.bestIndex) {
            if (logger.getEnabled())
              logger.log("For duplication " + duplication + " similarity " + similarity + " done inferring ancestors at offset " + offset + " because reached best index " + index);
            break;
          }
        }
      }
    }
  }

  private void write(Sequence sequence, int index, byte encodedAllele, SimilarityAnalysis similarityAnalysis) {
    OverriddenSequence overridden = this.sequenceOverrides.get(sequence);
    if (overridden == null) {
      throw new IllegalArgumentException("No overridable sequence created for sequence " + sequence.getName());
    }
    try {
      synchronized(overridden) {
        overridden.putEncoded(index, encodedAllele);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to write override position " + index + " on " + similarityAnalysis.sequence.getName() + " for analysis from " + similarityAnalysis.startIndex + " to " + similarityAnalysis.boundIndex, e);
    }
  }

  private Map.Entry<Integer, Duplication> getInterestingDuplicationStartingBefore(int index, TreeMap<Integer, Duplication> duplicationsHere) {
    while (true) {
      Map.Entry<Integer, Duplication> result = duplicationsHere.lowerEntry(index);
      if (result == null)
        return result;
      if (result.getValue().getNumInstances() >= 3) {
        // found an interesting duplication
        return result;
      }
      // this duplication isn't interesting so keep looking
      index = result.getKey();
    }
  }

  private Map.Entry<Integer, Duplication> getInterestingDuplicationStartingAfter(int index, TreeMap<Integer, Duplication> duplicationsHere) {
    while (true) {
      Map.Entry<Integer, Duplication> result = duplicationsHere.higherEntry(index);
      if (result == null)
        return result;
      if (result.getValue().getNumInstances() >= 3) {
        // found an interesting duplication
        return result;
      }
      // this duplication isn't interesting so keep looking
      index = result.getKey();
    }

  }

  private SimilarityAnalysis computeAnalysisBounds(Duplication duplication, SequencePosition startPosition, Readable_DuplicationDetector duplicationDetector, int polarity) {
    Sequence sequence = startPosition.getSequence();
    int startIndex = startPosition.getStartIndex();
    int endIndex = startIndex + duplication.getLength();
    TreeMap<Integer, Duplication> duplicationsHere = duplicationDetector.getInterestingDuplicationsOnSequence(sequence);

    // compute bound
    int duplicationMiddle = centerOfDuplication(startIndex, duplication.getLength());
    int duplicationMiddleOffset = duplication.getLength() / 2;
    int analysisInitialIndex;
    if (polarity > 0) {
      // we don't want to process the middle position twice so we process it when going backwards
      analysisInitialIndex = duplicationMiddle + 1;
    } else {
      analysisInitialIndex = duplicationMiddle;
    }
    int bound;
    if (polarity > 0) {
      bound = sequence.getLength();

      // Can't go past the middle of another duplication - instead split the space inbetween
      Map.Entry<Integer, Duplication> nextDuplication = getInterestingDuplicationStartingAfter(startIndex, duplicationsHere);
      if (nextDuplication != null) {
        int nextMiddle = centerOfDuplication(nextDuplication.getKey(), nextDuplication.getValue().getLength());
        bound = middleBetween(duplicationMiddle, nextMiddle) + 1;
      }
    } else {
      bound = -1;
      
      // Can't go past the middle of another duplication - instead split the space inbetween
      Map.Entry<Integer, Duplication> prevDuplication = getInterestingDuplicationStartingBefore(startIndex, duplicationsHere);
      if (prevDuplication != null) {
        // we don't want to process the middle position twice so we processed it when going forwards
        int prevMiddle = centerOfDuplication(prevDuplication.getKey(), prevDuplication.getValue().getLength());
        bound = middleBetween(prevMiddle, duplicationMiddle);
      }
    }
    SimilarityAnalysis result = new SimilarityAnalysis(sequence, analysisInitialIndex, bound, this.getMatchScore(duplication.getLength()));
    //System.err.println("computeAnalysisBounds for duplication length " + duplication.getLength() + " at " + sequence.getName() + "[" + startIndex + "], polarity = " + polarity + " result: startIndex = " + analysisInitialIndex + " bound = " + bound + " (" + result + ")");
    if ((result.boundIndex - result.startIndex) * polarity < 0) {
      return null; // If the polarity is backwards it means we found overlapping duplication of a different length, and the current duplication is considered to be not interesting
    }
    return result;
  }

  private double getMatchScore(int length) {
    return this.dissimilarityThreshold * length;
  }

  private double getMismatchScore(int length) {
    // We want to detect references that differ by up to dissimilarityThreshold (fraction of positions)
    // So, each position gets a bonus for existing, and each mismatched position gets a penalty for being mismatched
    return -length + getMatchScore(length);
  }

  private int middleBetween(int left, int right) {
    return (left + right) / 2;
  }

  private int centerOfDuplication(int start, int length) {
    return start + length / 2;
  }

  private Duplication getNextDuplicationToProcess(Readable_DuplicationDetector duplicationDetector, Logger logger) {
    Queue<Duplication> duplicationsToProcess = this.getDuplicationsToProcess(duplicationDetector, logger);

    synchronized(this) {
      int numDuplicationsRemaining = duplicationsToProcess.size();
      if (numDuplicationsRemaining < 1)
        return null;
      int numDuplicationsProcessed = this.totalNumDuplications - numDuplicationsRemaining;
      this.statusLogger.log("Processing duplication " + numDuplicationsProcessed + " of " + this.totalNumDuplications, false);
      return duplicationsToProcess.remove();
    }
  }

  private Queue<Duplication> getDuplicationsToProcess(Readable_DuplicationDetector duplicationDetector, Logger logger) {
    // check whether we've already found the duplications
    synchronized(this) {
      if (this.duplicationsToProcess != null) {
        return this.duplicationsToProcess;
      }
    }

    // search for duplications in parallel
    Set<Duplication> allDuplications = duplicationDetector.getAll();

    // update our result queue and return it
    boolean duplicationsAreNew = false;
    synchronized(this) {
      if (this.duplicationsToProcess == null) {
        this.duplicationsToProcess = new ArrayDeque<Duplication>(allDuplications);
        duplicationsAreNew = true;
        this.totalNumDuplications = allDuplications.size();
      }
    }
    if (duplicationsAreNew) {
      System.err.println("AncestryDetector processing " + this.duplicationsToProcess.size() + " duplications");
      if (logger.getEnabled()) {
        logger.log("AncestryDetector processing " + this.duplicationsToProcess.size() + " duplications");
        for (Duplication duplication: allDuplications) {
          logger.log(" " + duplication);
        }
      }
    }
    return this.duplicationsToProcess;
  }

  private List<Sequence> reference;
  private DuplicationDetector duplicationDetector;

  private Queue<Duplication> duplicationsToProcess;
  private int totalNumDuplications;
  private HashBlock_Database result;
  private int numActiveWorkers;

  private Map<Sequence, OverriddenSequence> sequenceOverrides;
  private Map<Sequence, Sequence> sequencesByOverride;
  private double dissimilarityThreshold;

  private FastaWriter outputWriter;
  private StatusLogger statusLogger;
  private boolean verifyNoDuplicateAnalyses;

  private boolean resultingDatabaseEnableGapmers = true;
}
