package mapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class AlignerWorker extends Thread {
  static Logger silentLogger = Logger.NoOpLogger;

  public AlignerWorker(ReferenceProvider referenceProvider, AlignmentParameters parameters, Readable_DuplicationDetector duplicationDetector, int workerId, List<AlignmentListener> resultsListeners, AlignmentCache resultsCache, Queue completionListener) {
    this.referenceProvider = referenceProvider;
    this.parameters = parameters;
    this.workerId = "" + workerId;
    while (this.workerId.length() < 5) {
      this.workerId = " " + this.workerId;
    }
    this.resultsCache = resultsCache;
    this.resultsListeners = resultsListeners;
    this.completionListener = completionListener;
    this.duplicationDetector = duplicationDetector;
  }

  // for tests
  public void setLogger(Logger logger) {
    this.logger = logger;
    this.referenceLogger = logger;
    this.detailedAlignmentLogger = logger.incrementScope();
  }

  public void requestProcess(List<QueryBuilder> queries, long startMillis, long estimatedTotalNumQueries, Logger alignmentLogger, Logger referenceLogger) {
    this.resetStatistics();
    this.estimatedTotalNumQueries = estimatedTotalNumQueries;

    this.startMillis = startMillis;
    this.logger = alignmentLogger;
    this.referenceLogger = referenceLogger;
    this.detailedAlignmentLogger = alignmentLogger.incrementScope();
    this.queries = queries;

    try {
      this.workQueue.put(true);
    } catch (InterruptedException e) {
      throw new IllegalArgumentException("Worker "  + this.workerId + " has no capacity for more work");
    }
  }

  private void resetStatistics() {
    numCacheHits = 0;
    numCacheMisses = 0;

    slowestQuery = null;
    slowestAlignment = null;
    slowestAlignmentMillis = -1;
    queryAtRandomMoment = null;
    randomMomentSelector = new RandomMomentSelector();
    millisSpentOnUnalignedQueries = 0;
    millisSpentAligningMatches = 0;
    millisThroughOptimisticBestAlignments = 0;
    numCasesImmediatelyAcceptingFirstAlignment = 0;
    numIndels = 0;
  }

  public void noMoreQueries() {
    try {
      this.workQueue.put(false);
    } catch (InterruptedException e) {
      throw new IllegalArgumentException("Worker " + this.workerId + " is still working");
    }
  }

  @Override
  public void run() {
    this.setup();
    while (true) {
      boolean succeeded = false;
      Boolean moreWork = false;
      try {
        moreWork = this.workQueue.take();
      } catch (InterruptedException e) {
        if (this.logger.getEnabled())
          this.logger.log("Interrupted");
        break;
      }
      if (!moreWork)
        break;
      try {
        this.process();
        succeeded = true;
      } finally {
        // record error if any
        if (!succeeded)
          this.failed = true;
        this.completionListener.add(this);
      }
    }
  }

  public void setup() {
    // create the reference if we haven't already
    HashBlock_Database hashblockDatabase = this.referenceProvider.get_HashBlock_database(this.referenceLogger);
    this.referenceDatabase = hashblockDatabase.getView();
    this.sequenceDatabase = hashblockDatabase.getSequenceDatabase();
    this.shortestHashblockLength = hashblockDatabase.getMinInterestingSize();
    this.duplicationDetector.helpSetup();
  }

  private void process() {

    // First figure out how often to enable the cache
    // If we have evidence that the cache always works, then we want the cache to always be enabled.
    // If the cache has never worked, we want to enable it occasionally (n^(1/3) times) to double check whether it can work.
    // If we have no data, we want to enable the cache a little bit to check whether it can work.

    double numCacheHits = this.resultsCache.getNumHits();
    double numSavedResults = this.resultsCache.getUsage();
    double estimatedNewNumSavedResults = numSavedResults + Math.pow(this.queries.size(), 1.0/3.0);

    double cacheEnableFraction = (numCacheHits * numCacheHits + 1.0) / (estimatedNewNumSavedResults * estimatedNewNumSavedResults + 1);
    if (cacheEnableFraction > 1)
      cacheEnableFraction = 1;

    long numHashcodesToCache = (long)((double)(Integer.MAX_VALUE) * (double)(cacheEnableFraction) * 2);
    this.maxHashcodeToCache = Integer.MIN_VALUE + numHashcodesToCache;

    if (logger.getEnabled()) {
      logger.log("Num cache hits = " + numCacheHits + " num cache entries = " + numSavedResults + " num queries = " + this.queries.size() + "; cache enabled fraction = " + cacheEnableFraction + ", using cache for hashcodes up to " + this.maxHashcodeToCache);
    }

    if (this.queries.size() < 1) {
      // We're just supposed to help the HashBlock_Database hash its sequences
      while(true) {
        this.referenceDatabase.helpSetUp();
        if (!this.referenceDatabase.getCanUseHelp()) {
          break;
        }
      }
    } else {
      // make sure we've hashed the reference before we start running queries, so that running the queries probably won't require hashing the reference and our timing measurements can be approximately accurate
      this.referenceDatabase.prepare();
      // now that we've hashed the reference, we can run queries
      // List that for each query says where it aligns
      List<QueryAlignments> alignments = new ArrayList<QueryAlignments>();
      for (QueryBuilder queryBuilder : queries) {
        long start = System.currentTimeMillis();
        this.latestQueryAlignmentStart = start;
        Query query = queryBuilder.build();
        QueryAlignments alignmentsHere;
        int previousHashedLength = this.referenceDatabase.getHashedLength();
        try {
          alignmentsHere = this.align(query);
          int newHashedLength = this.referenceDatabase.getHashedLength();
          // If we spent any time increasing the max length included in the hashblock database, then we don't really want to attribute that time to this particular query, because it should happen rarely and might also happen for other queries too
          // So, if we increased the max hashed length in the hashblock database, then we rerun the query and time how long it takes to run it the second time
          if (newHashedLength != previousHashedLength) {
            start = System.currentTimeMillis();
            if (logger.getEnabled()) {
              logger.log("Re-running query to determine how long it takes to align this query without the time required to hash the reference");
            }
            alignmentsHere = this.alignWithoutCache(query);
          }
        } catch (Exception e) {
          throw new RuntimeException("Failed to align " + query.format(), e);
        }
        // update some timing information
        long end = System.currentTimeMillis();
        long elapsed = end - start;
        if (elapsed > this.slowestAlignmentMillis) {
          this.slowestAlignmentMillis = (int)elapsed;
          this.slowestQuery = query;
          this.slowestAlignment = alignmentsHere;
        }
        if (this.randomMomentSelector.select(end)) {
          this.queryAtRandomMoment = query;
        }
        // collect results
        alignments.add(alignmentsHere);
        for (HashMap.Entry<Query, List<QueryAlignment>> componentAlignments: alignmentsHere.getAlignments().entrySet()) {
          Query subQuery = componentAlignments.getKey();
          List<QueryAlignment> subAlignments = componentAlignments.getValue();
          numIndels += countNumIndels(subAlignments);

          if (subAlignments.size() > 0) {
            if (this.logger.getEnabled()) {
              this.printAlignment(subQuery, subAlignments);
            }
          } else {
            this.millisSpentOnUnalignedQueries += elapsed;
            if (this.logger.getEnabled()) {
              log("Unaligned    : " + query.format());
            }
          }
        }
        if (this.logger.getEnabled()) {
          log(" ");
        }
      }
      this.sendResults(alignments);
    }
  }

  private int countNumIndels(List<QueryAlignment> alignmentOptions) {
    int numIndels = 0;
    for (QueryAlignment option: alignmentOptions) {
      for (SequenceAlignment sequenceAlignment: option.getComponents()) {
        numIndels = Math.max(numIndels, sequenceAlignment.countNumIndels());
      }
    }
    return numIndels;
  }

  public boolean tryComplete() throws InterruptedException {
    this.referenceLogger.flush();
    this.logger.flush();
    return !this.failed;
  }

  private void log(String message) {
    this.logger.log(message);
  }

  public QueryAlignments align(Query query) {
    query.decompress();
    QueryAlignments result = this.checkCacheAndAlign(query);
    query.compress();
    return result;
  }

  // checks the cache and aligns the query
  private QueryAlignments checkCacheAndAlign(Query query) {
    int hashCode = query.hashCode();
    if (hashCode > this.maxHashcodeToCache) {
      // if the cache hasn't been doing well lately, we don't use it as much
      return this.alignWithoutCache(query);
    }
    // If the cache seems useful then we use it more
    QueryAlignments cached = this.resultsCache.get(query);
    if (cached != null) {
      // we currently only support reusing cache results for queries that don't split during alignment
      if (cached.getNumQueries() == 1 && cached.getFirstQuery().getNumSequences() == query.getNumSequences()) {
        numCacheHits++;
        List<QueryAlignment> firstComponent = cached.getFirstAlignments();
        List<QueryAlignment> newComponent = new ArrayList<QueryAlignment>(firstComponent.size());
        for (QueryAlignment option: firstComponent) {
          newComponent.add(option.withQuery(query));
        }
        return new QueryAlignments(query, newComponent);
      }
    }
    QueryAlignments result = this.alignWithoutCache(query);
    this.resultsCache.addAlignment(query, result);
    return result;
  }

  // aligns to the unmodified reference we've been given
  public QueryAlignments alignWithoutCache(Query query) {
    QueryAlignments results = this.alignToAncestralReference(query);
    for (List<QueryAlignment> subAlignments: results.getAlignments().values()) {
      for (QueryAlignment alignment: subAlignments) {
        this.updateSequenceB(alignment);
      }
    }

    return results;
  }

  // aligns to the ancestral reference
  private QueryAlignments alignToAncestralReference(Query query) {
    if (this.logger.getEnabled()) {
      log("Aligning      " + query.format());
    }
    double maxInterestingPenalty = query.getLength() * parameters.MaxErrorRate;
    int maxInnerDistance = (int)(maxInterestingPenalty * query.getSpacingDeviationPerUnitPenalty() + query.getExpectedInnerDistance());

    List<Counting_HashBlockPath> components = new ArrayList<Counting_HashBlockPath>(query.getNumSequences());
    String componentName = "seq";
    for (int i = 0; i < query.getNumSequences(); i++) {
      Sequence querySequence = query.getSequence(i);
      if (i > 0)
        querySequence = querySequence.reverseComplement();
      if (query.getNumSequences() > 1)
        componentName = "seq" + (components.size() + 1);
      HashBlock_Stream stream = new HashBlock_Stream(querySequence, false, null);
      HashBlock_Pyramid pyramid = new HashBlock_Pyramid(stream);
      Counting_HashBlockPath component = new Counting_HashBlockPath(pyramid, referenceDatabase, sequenceDatabase, querySequence, componentName, this.detailedAlignmentLogger, parameters);
      components.add(component);
    }
    HashBlockPaths_Counter path = new HashBlockPaths_Counter(components, (int)query.getExpectedInnerDistance(), maxInnerDistance, this.detailedAlignmentLogger);

    QueryAlignment optimisticBestAlignment = null;
    QueryMatch optimisticBestMatch = null;
    // group the matches by their sequence and count, and find the most popular
    int numMismatches = 0;

    // check the first match
    List<QueryMatch> bestMatches = path.optimisticGetBestMatches();
    QueryMatch_Aligner aligner = new QueryMatch_Aligner(query, this.parameters, logger);
    if (bestMatches.size() == 1) {
      optimisticBestMatch = bestMatches.get(0);
      optimisticBestAlignment = this.alignMatch(optimisticBestMatch, aligner);
      if (logger.getEnabled()) {
        if (optimisticBestAlignment != null) {
          logger.log("Optimistic best alignment at " + optimisticBestMatch.summarizePositionB() + " with penalty " + optimisticBestAlignment.getPenalty());
        } else {
          logger.log("Optimistic best match at " + optimisticBestMatch.summarizePositionB() + " but couldn't align");
        }
      }
      long duration = System.currentTimeMillis() - this.latestQueryAlignmentStart;
      this.millisThroughOptimisticBestAlignments += duration;

      // Temporarily disabled because of high memory requirements
      if (quicklyConfidentInBestAlignment(optimisticBestAlignment, optimisticBestMatch)) {
        numCasesImmediatelyAcceptingFirstAlignment++;
        return new QueryAlignments(query, optimisticBestAlignment);
      }
    }

    if (optimisticBestAlignment != null) {
      // see if we can show that the first match is the best
      while (true) {
        // check whether we can prove that the first alignment that we checked is also the best
        double possiblePenalty = this.getPenaltyLowerBound(numMismatches);
        if (possiblePenalty > optimisticBestAlignment.getPenalty() + parameters.Max_PenaltySpan) {
          if (logger.getEnabled()) {
            if (optimisticBestAlignment.getPenalty() == 0) {
              log("Exactly one position matches the beginning of the query, and the query aligns there perfectly");
            } else {
              log("One position matches the beginning of the query better than any other location, and no other location should be able to align better overall (min number of possible mismatches elsewhere >= " + numMismatches + ")");
            }
          }

          numCasesImmediatelyAcceptingFirstAlignment++;
          return new QueryAlignments(query, optimisticBestAlignment);
        } else {
          if (this.detailedAlignmentLogger.getEnabled()) {
            this.detailedAlignmentLogger.log("cannot prove optimistic alignment is best yet: penalty could be " + possiblePenalty + " for " + numMismatches + " distinct mismatched blocks");
          }
        }
        List<QueryMatch> matches = path.findGoodPositionsHavingPriority(numMismatches);
        numMismatches++;
        boolean done = false;
        for (QueryMatch match: matches) {
          if (!optimisticBestMatch.samePosition(match)) {
            if (logger.getEnabled()) {
              log("There might be another alignment position with less penalty than " + optimisticBestMatch.summarizePositionB() + " (" + optimisticBestAlignment.getPenalty() + "), including " + match.summarizePositionB());
            }

            done = true;
            break;
          }
        }
        if (done)
          break;
      }
    }

    // If we get here then we're not sure whether the first alignment we found will be the best
    // So, we check more offsets, and we order our search by their number of mismatched hashblocks
    double bestPenalty = Integer.MAX_VALUE;
    int candidateNumMismatches = 0;

    while (true) {
      double estimatedPenalty = getPenaltyLowerBound(candidateNumMismatches);

      // We're not interested in alignments having more penalty than the highest we've found so far
      if (estimatedPenalty > bestPenalty + parameters.Max_PenaltySpan) {
        if (logger.getEnabled()) {
          logger.log("Done checking alignment positions: " + candidateNumMismatches + " mismatches implies penalty " + estimatedPenalty + " which is more than bestPenalty " + bestPenalty);
        }
        break;
      }
      // If we've checked all matching positions, we're done
      if (candidateNumMismatches > path.getNumBlocks()) {
        if (logger.getEnabled()) {
          logger.log("Done checking alignment positions: candidateNumMismatches = " + candidateNumMismatches + ", path.getNumBlocks() = " + path.getNumBlocks());
        }

        break;
      }

      // Check positions having the current number of mismatches that we're expecting
      List<QueryMatch> candidates = path.findGoodPositionsHavingPriority(candidateNumMismatches);
      if (logger.getEnabled()) {
        if (candidates.size() > 0) {
          log("Checking positions having number of hashblock mismatches = " + candidateNumMismatches);
        }
      }
      for (QueryMatch match : candidates) {
        QueryAlignment alignment;
        if (optimisticBestMatch != null && match.samePosition(optimisticBestMatch)) {
          if (logger.getEnabled())
            logger.log("Already computed alignment for " + match.summarizePositionB());
          alignment = optimisticBestAlignment;
        } else {
          alignment = this.alignMatch(match, aligner);
        }
        if (alignment != null) {
          double penalty = alignment.getPenalty();
          if (bestPenalty > penalty)
            bestPenalty = penalty;
        }
      }
      // If the estimated penalty of this alignment is the maximum interesting penalty, then we don't need to check alignments having even more mismatched hashblocks
      if (estimatedPenalty >= maxInterestingPenalty) {
        if (logger.getEnabled()) {
          logger.log("Done checking alignment positions: estimatedPenalty = " + estimatedPenalty + " maxInterestingPenalty = " + maxInterestingPenalty);
        }
        break;
      }

      // Continue on to check positions having more mismatched hashblocks
      candidateNumMismatches++;
    }

    if (aligner.getBestAlignments().size() < 1 && query.getNumSequences() > 1) {
      if (logger.getEnabled())
        logger.log("Found no alignments having good support. Looking for alignments with some support");
      List<QueryMatch> partiallyGoodPositions = path.findPartiallyGoodPositions();
      if (logger.getEnabled())
        logger.log("Found " + partiallyGoodPositions.size() + " positions having some support");

      for (QueryMatch match: partiallyGoodPositions) {
        QueryAlignment alignment = this.alignMatch(match, aligner);
        if (alignment != null) {
          double penalty = alignment.getPenalty();
          if (bestPenalty > penalty)
            bestPenalty = penalty;
        }
      }
    }

    List<QueryAlignment> bestAlignments = aligner.getBestAlignments();

    QueryAlignments result = new QueryAlignments(query, bestAlignments);
    if (bestAlignments.size() < 1 && query.getNumSequences() > 1) {
      result = getUnpairedAlignments(query, path);
    }

    if (bestAlignments.size() > parameters.MaxNumMatches) {
      if (logger.getEnabled()) {
        log("Found more alignments than requested: " + bestAlignments.size() + " > " + parameters.MaxNumMatches + "; clearing results");
      }
      return new QueryAlignments(query);
    }

    return result;
  }


  private double getPenaltyLowerBound(int numMismatchedHashblocks) {
    double mutationPenalty = numMismatchedHashblocks * parameters.MutationPenalty;
    double indelPenalty = this.shortestHashblockLength * numMismatchedHashblocks * parameters.DeletionExtension_Penalty;
    return Math.min(mutationPenalty, indelPenalty);
  }

  // Try to prove that this is the best alignment
  private boolean quicklyConfidentInBestAlignment(QueryAlignment optimisticBestAlignment, QueryMatch optimisticBestMatch) {
    if (optimisticBestAlignment == null)
      return false;
    // If it has indels then the offset might be wrong and we can't prove that it's right
    if (optimisticBestAlignment.hasIndel())
      return false;

    // Check whether there are any duplications in the reference near this position
    Sequence alignedReference = optimisticBestMatch.getComponent(0).getSequenceB();

    Sequence originalReference = this.referenceProvider.getOriginalSequence(alignedReference);

    // Find neighboring duplications
    int matchStart = optimisticBestMatch.getStartIndexB();
    int matchEnd = optimisticBestMatch.getEndIndexB();
    boolean hasNearbyDuplication = false;
    // We want to prove that there cannot be a better alignment to the reference somewhere else
    // We try to do this by trying to prove that there is no other section of the reference that is similar to this one
    // If our alignment has P penalty, we want to prove that there is no other part of the reference that is less than 2*P penalty away from the part of the reference used in this alignment
    // Technically the following analysis isn't strictly proof, but in practice it tends to work well and tends to save a lot of time that we spend on accuracy in other ways.

    // how much distance per mutation is required for us to detect it
    double similarityDetectionGranularity = duplicationDetector.getDetectionGranularity();
    // logger.log("similarity detection granularity = " + similarityDetectionGranularity);

    // observed error rate
    double numberOfMutations = optimisticBestAlignment.getPenalty() / parameters.MutationPenalty;
    // logger.log("number of mutations = " + numberOfMutations);
    double existingMutationRate = numberOfMutations / optimisticBestMatch.getQueryTotalLength();
    // logger.log("existing mutation rate = " + existingMutationRate);
    if (existingMutationRate <= 0)
      return true; // perfect score

    // the probability that a specific block in the similar section is not perfectly duplicated
    double probabilityMutationInSection = 1 - Math.pow(1 - existingMutationRate, similarityDetectionGranularity);
    // logger.log("probability mutation in section = " + probabilityMutationInSection);

    // the probability that we're willing to accept a false negative where there is a similar section but we don't notice it
    double acceptableProbability = 1.0 / sequenceDatabase.getTotalForwardAndReverseSize();
    // logger.log("acceptable probability of false negatives = " + acceptableProbability);

    double numberOfUnmatchedBlocksForHighConfidence = Math.log(acceptableProbability) / Math.log(probabilityMutationInSection);
    // logger.log("number of unmatched blocks required for high confidence = " + numberOfUnmatchedBlocksForHighConfidence);

    double totalLengthForHighConfidence = numberOfUnmatchedBlocksForHighConfidence * similarityDetectionGranularity;
    // logger.log("total length required for high confidence = " + totalLengthForHighConfidence);

    double matchMiddle = (matchStart + matchEnd) / 2;
    double interestingWindow = Math.max(totalLengthForHighConfidence, (matchEnd - matchStart + 1) / 2);

    int windowStart = (int)(matchMiddle - interestingWindow);
    int windowEnd = (int)(matchMiddle + interestingWindow);


    Integer duplicationIndex = duplicationDetector.mayContainDuplicationInRange(originalReference, windowStart, windowEnd);


    if (duplicationIndex != null) {
      if (logger.getEnabled()) {
        logger.log("Match at " + optimisticBestMatch.summarizePositionB() + " is within approximately " + interestingWindow + " of duplication at: " + duplicationIndex);
      }
      hasNearbyDuplication = true;
    } else {
      if (matchStart <= interestingWindow) {
        if (logger.getEnabled()) {
          logger.log("Match at " + optimisticBestMatch.summarizePositionB() + " is within " + interestingWindow + " of start of contig");
        }
        hasNearbyDuplication = true;
      } else {
        if (matchEnd >= originalReference.getLength() - interestingWindow) {
          if (logger.getEnabled()) {
            logger.log("Match at " + optimisticBestMatch.summarizePositionB() + " is within " + interestingWindow + " of end of contig");
          }
          hasNearbyDuplication = true;
        }
      }
    }

    if (hasNearbyDuplication)
      return false;
    if (logger.getEnabled()) {
      logger.log("Match at " + optimisticBestMatch.summarizePositionB() + " isn't within " + interestingWindow + " basepairs of any significant duplications in the reference");
    }
    if (optimisticBestAlignment.hasAmbiguousBasepairs()) {
      if (logger.getEnabled())
        logger.log("Match at " + optimisticBestMatch.summarizePositionB() + " contains ambiguous basepairs so we're not confident that we would detect duplication");
      return false;
    }
    return true;
  }

  private QueryAlignment alignMatch(QueryMatch match, QueryMatch_Aligner aligner) {
    return this.alignMatch(match, 0, aligner);
  }

  private QueryAlignment alignMatch(QueryMatch match, double extraSpacing, QueryMatch_Aligner aligner) {
    long start = System.currentTimeMillis();
    QueryAlignment result = aligner.align(match, extraSpacing);
    long end = System.currentTimeMillis();
    long elapsed = end - start;
    this.millisSpentAligningMatches += elapsed;
    return result;
  }

  private QueryAlignments getUnpairedAlignments(Query query, HashBlockPaths_Counter path) {
    if (logger.getEnabled()) {
      log("Checking for unpaired alignments");
    }
    Map<Query, List<QueryAlignment>> partialAlignments = new LinkedHashMap<Query, List<QueryAlignment>>();
    double bestComponentPenalty = Integer.MAX_VALUE;
    double expectedInnerDistance = query.getExpectedInnerDistance();
    for (int sequenceIndex = 0; sequenceIndex < query.getNumSequences(); sequenceIndex++) {
      Sequence sequence = query.getSequence(sequenceIndex);
      double maxInterestingSubqueryPenalty = sequence.getLength() * parameters.MaxErrorRate;
      int maxNumMutations = (int)(maxInterestingSubqueryPenalty / parameters.MutationPenalty);
      int maxNumMismatches = maxNumMutations;

      // check each candidate match location
      List<SequenceMatch> candidateLocations = path.findGoodComponentMatches(sequenceIndex, maxNumMismatches);
      Query subQuery = query.subquery(sequenceIndex);
      QueryMatch_Aligner subqueryAligner = new QueryMatch_Aligner(subQuery, parameters, logger);
      for (SequenceMatch sequenceMatch: candidateLocations) {
        int minInnerDistance;
        if (sequenceIndex % 2 == 1) {
          minInnerDistance = sequenceMatch.getStartIndexB();
        } else {
          minInnerDistance = sequenceMatch.sequenceB.getLength() - sequenceMatch.getEndIndexB();
        }
        double innerDistance = minInnerDistance;
        if (innerDistance < expectedInnerDistance)
          innerDistance = expectedInnerDistance;
        double spacingPenalty = innerDistance / query.getSpacingDeviationPerUnitPenalty();
        if (spacingPenalty > maxInterestingSubqueryPenalty)
          continue;

        // We found a location near the edge of a contig - now we can try to align it
        QueryMatch subqueryMatch = new QueryMatch(sequenceMatch, -1);
        this.alignMatch(subqueryMatch, innerDistance, subqueryAligner);
      }
      List<QueryAlignment> componentAlignments = subqueryAligner.getBestAlignments();
      partialAlignments.put(subQuery, componentAlignments);
    }
    return new QueryAlignments(partialAlignments);
  }

  void printAlignment(Query query, List<QueryAlignment> alignments) {
    for (QueryAlignment alignment: alignments) {
      log(alignment.formatVerbose());
    }
  }

  private void sendResults(List<QueryAlignments> results) {
    for (AlignmentListener listener : this.resultsListeners) {
      listener.addAlignments(results);
    }
    this.resultsCache.addHits(this.numCacheHits);
  }

  public Query getSlowestQuery() {
    return this.slowestQuery;
  }
  public QueryAlignments getSlowestAlignment() {
    return this.slowestAlignment;
  }
  public int getSlowestAlignmentMillis() {
    return this.slowestAlignmentMillis;
  }
  public Query getQueryAtRandomMoment() {
    return this.queryAtRandomMoment;
  }
  public long getMillisSpentOnUnalignedQueries() {
    return this.millisSpentOnUnalignedQueries;
  }
  public long getMillisSpentAligningMatches() {
    return this.millisSpentAligningMatches;
  }
  public long getMillisThroughOptimisticBestAlignments() {
    return this.millisThroughOptimisticBestAlignments;
  }
  public int getNumCacheHits() {
    return this.numCacheHits;
  }
  public int getNumCasesImmediatelyAcceptingFirstAlignment() {
    return this.numCasesImmediatelyAcceptingFirstAlignment;
  }
  public long getNumIndels() {
    return this.numIndels;
  }

  private void updateSequenceB(QueryAlignment queryAlignment) {
    Sequence computedSequenceB = queryAlignment.getSequenceB();
    Sequence originalSequence = this.referenceProvider.getOriginalSequence(computedSequenceB);
    queryAlignment.putSequenceB(originalSequence);
  }

  ReferenceProvider referenceProvider;
  SequenceDatabase sequenceDatabase;
  Readable_HashBlock_Database referenceDatabase;
  Readable_DuplicationDetector duplicationDetector;
  int shortestHashblockLength;
  List<AlignmentListener> resultsListeners;
  AlignmentParameters parameters;
  Logger logger;
  Logger detailedAlignmentLogger;
  Logger referenceLogger;
  String workerId;
  long startMillis;
  boolean failed = false;
  List<SequenceMatch> emptyMatchList = new ArrayList<SequenceMatch>(0);
  int numCacheHits;
  int numCacheMisses;
  long numIndels;

  Query slowestQuery;
  Query queryAtRandomMoment;
  RandomMomentSelector randomMomentSelector;
  QueryAlignments slowestAlignment;
  int slowestAlignmentMillis = -1;
  long millisSpentOnUnalignedQueries;
  long millisSpentAligningMatches;
  long millisThroughOptimisticBestAlignments;
  long latestQueryAlignmentStart;
  int numCasesImmediatelyAcceptingFirstAlignment;
  Queue<AlignerWorker> completionListener;
  List<QueryBuilder> queries;

  BlockingQueue<Boolean> workQueue = new ArrayBlockingQueue<Boolean>(1);
  AlignmentCache resultsCache;
  long estimatedTotalNumQueries;
  long maxHashcodeToCache;
}
