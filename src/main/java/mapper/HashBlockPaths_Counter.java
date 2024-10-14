package mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

// a HashBlockPaths_Counter keeps track of some metrics about several HashBlockPaths
public class HashBlockPaths_Counter {
  public HashBlockPaths_Counter(List<Counting_HashBlockPath> components, int expectedInnerDistance, int maxInnerDistanceBetweenComponents, Logger logger) {
    this.components = components;
    this.maxOffsetBetweenComponents = maxInnerDistanceBetweenComponents + components.get(0).getQuerySequence().getLength();
    this.expectedOffsetBetweenComponents = expectedInnerDistance + components.get(0).getQuerySequence().getLength();
    this.logger = logger;
  }

  // returns a list of the best positions where the query could align
  public List<QueryMatch> findGoodPositionsHavingPriority(int numMismatches) {
    List<QueryMatch> allMatches = this.findGoodPositionsWithPriorityUpTo(numMismatches);
    return this.filterMatchesHavingPriority(allMatches, numMismatches);
  }

  public List<QueryMatch> findPartiallyGoodPositions() {
    if (this.components.size() != 2) {
      return new ArrayList<QueryMatch>(0);
    }
    if (!this.foundNonemptyResult) {
      return new ArrayList<QueryMatch>(0);
    }
    List<List<HashBlockMatch_Counter>> pieces = new ArrayList<List<HashBlockMatch_Counter>>(this.components.size());
    boolean foundGoodPosition = false;
    boolean foundBadPosition = false;
    for (Counting_HashBlockPath component: this.components) {
      List<HashBlockMatch_Counter> matchesHere = component.findGoodPositionsHavingPriorityUpTo(Integer.MAX_VALUE);
      if (matchesHere.size() == 0) {
        foundBadPosition = true;
        matchesHere = component.getAllPositions();
      } else {
        foundGoodPosition = true;
      }
      pieces.add(matchesHere);
    }
    if (foundGoodPosition && foundBadPosition)
      return this.match(pieces);
    return new ArrayList<QueryMatch>(0);
  }

  private List<QueryMatch> findGoodPositionsWithPriorityUpTo(int numMismatches) {
    List<List<HashBlockMatch_Counter>> pieces = new ArrayList<List<HashBlockMatch_Counter>>(this.components.size());
    for (Counting_HashBlockPath component : this.components) {
      int componentIndex = pieces.size();
      List<HashBlockMatch_Counter> matchesHere = component.findGoodPositionsHavingPriorityUpTo(numMismatches);
      if (this.logger.getEnabled() && this.components.size() > 1) {
        int numPositionsWithThresholdNumberMismatches = 0;
        for (HashBlockMatch_Counter counter: matchesHere) {
          if (counter.getPriority() == numMismatches) {
            numPositionsWithThresholdNumberMismatches++;
          }
        }
        if (numPositionsWithThresholdNumberMismatches > 0) {
          logger.log(component.getQueryShortName() + " found " + numPositionsWithThresholdNumberMismatches + " good positions having priority " + numMismatches);
          for (HashBlockMatch_Counter counter: matchesHere) {
            if (counter.getPriority() == numMismatches) {
              logger.log(counter.getMatch().getSequenceB().getName() + " offset " + counter.getMatch().getOffset() + ": " + counter.getNumDistinctMismatches() + " mismatches");
            }
          }
        }
      }
      if (matchesHere.size() < 1) {
        if (logger.getEnabled())
          logger.log("" + component.getQueryShortName() + " doesn't have any good positions with priority <= " + numMismatches);
      } else {
        this.foundNonemptyResult = true;
      }
      pieces.add(matchesHere);
    }
    return this.match(pieces);
  }

  // tries to get the list of best matches but can be incorrect
  public List<QueryMatch> optimisticGetBestMatches() {
    List<List<HashBlockMatch_Counter>> pieces = new ArrayList<List<HashBlockMatch_Counter>>(this.components.size());
    for (Counting_HashBlockPath component : this.components) {
      while (true) {
        // advance this component until it finds a match, and then use the result
        List<HashBlockMatch_Counter> best = component.getBestMatches();
        if (best.size() == 1 || !component.step()) {
          pieces.add(best);
          break;
        }
      }
    }
    List<QueryMatch> allMatches = this.match(pieces);
    return this.filterMatchesHavingMinPriority(allMatches);
  }

  // Returns a list of the best positions where the given single component (sequence) of the query could align
  // Doesn't incorporate spacing penalty (might return matches in the middle of a contig)
  public List<SequenceMatch> findGoodComponentMatches(int sequenceIndex, int maxPriority) {
    List<HashBlockMatch_Counter> componentMatches = this.components.get(sequenceIndex).findGoodPositionsHavingPriorityUpTo(maxPriority);
    List<SequenceMatch> sequenceMatches = matchCountersToSequenceMatches(componentMatches);
    return sequenceMatches;
  }

  public int getNumBlocks() {
    int total = 0;
    for (Counting_HashBlockPath component: this.components) {
      total += component.getNumBlocks();
    }
    return total;
  }

  private List<QueryMatch> match(List<List<HashBlockMatch_Counter>> components) {
    boolean same = true;
    if (this.previousMatchComponents == null) {
      same = false;
    } else {
      for (int i = 0; i < components.size(); i++) {
        if (this.previousMatchComponents.get(i) != components.get(i)) {
          same = false;
          break;
        }
      }
    }
    if (!same) {
      this.previousAssembledMatches = this.matchWithoutCache(components);
      this.previousMatchComponents = components;
    }
    return this.previousAssembledMatches;
  }

  // given a List<PossibleSequenceMatches>, matches up nearby sequence matches, and makes a QueryMatch for each
  private List<QueryMatch> matchWithoutCache(List<List<HashBlockMatch_Counter>> components) {
    if (components.size() > 2) {
      throw new IllegalArgumentException("It is currently only supported to match 2 query ends at a time, not " + components.size());
    }

    if (components.size() == 1) {
      List<QueryMatch> matches = new ArrayList<QueryMatch>(components.size());
      List<HashBlockMatch_Counter> possibilities = components.get(0);
      for (HashBlockMatch_Counter possibility : possibilities) {
        matches.add(new QueryMatch(possibility.getMatch(), possibility.getPriority()));
      }
      return matches;
    }

    // We have to match up each pair of nearby potential sequence matches even if their total number of hashblock mismatches is higher than the penalty limit,
    // because hashblock mismatches shared between two different sequence matches might not be distinct (two paired-end reads might overlap and each share the same hashblock mismatch)

    // LinkedHashMap<reference contig, TreeMap<offset, List<nearby HashBlockMatch_Counter>>>
    LinkedHashMap<Sequence, TreeMap<Integer, HashBlockMatch_Counter>> forwardMatchingComponents = new LinkedHashMap<Sequence, TreeMap<Integer, HashBlockMatch_Counter>>();
    LinkedHashMap<Sequence, TreeMap<Integer, HashBlockMatch_Counter>> reverseMatchingComponents = new LinkedHashMap<Sequence, TreeMap<Integer, HashBlockMatch_Counter>>();

    // essentially a List<QueryMatch>
    List<List<HashBlockMatch_Counter>> matchedCounters = new ArrayList<List<HashBlockMatch_Counter>>();

    boolean lastComponentIsLargest = components.size() <= 1 || components.get(0).size() <= components.get(1).size();
    // check each query sequence
    List<QueryMatch> guessedMatches = new ArrayList<QueryMatch>();
    for (int i = 0; i < components.size(); i++) {
      int componentIndex;
      if (lastComponentIsLargest)
        componentIndex = i;
      else
        componentIndex = 1 - i;

      List<HashBlockMatch_Counter> choices = components.get(componentIndex);

      // check each position where that query sequence could align
      for (HashBlockMatch_Counter counter: choices) {
        // search for this match in our data structure
        SequenceMatch match = counter.getMatch();
        Sequence referenceSequence = match.getSequenceB();
        // allow the two query sequences to be shifted in the opposite direction by up to this much
        int querySequenceLength = match.getSequenceA().getLength();
        int maxReverseOffset = querySequenceLength / 2;

        // separate forward matches from reverse matches
        LinkedHashMap<Sequence, TreeMap<Integer, HashBlockMatch_Counter>> matchingComponents;
        // determine whether this SequenceMatch corresponds to a forward or reverse QueryMatch
        boolean sequenceMatchReversed = match.getReversed();
        boolean queryMatchReversed = (sequenceMatchReversed == (componentIndex % 2 == 0));
        if (queryMatchReversed) {
          matchingComponents = reverseMatchingComponents;
        } else {
          matchingComponents = forwardMatchingComponents;
        }
        TreeMap<Integer, HashBlockMatch_Counter> matchesOnThisSequence = matchingComponents.get(referenceSequence);
        if (matchesOnThisSequence == null) {
          matchesOnThisSequence = new TreeMap<Integer, HashBlockMatch_Counter>();
          matchingComponents.put(referenceSequence, matchesOnThisSequence);
        }
        int offset = match.getOffset();
        if (i == 0) {
          if (i == components.size() - 1) {
            // this is the only component
            List<HashBlockMatch_Counter> group = new ArrayList<HashBlockMatch_Counter>(1);
            group.add(counter);
            matchedCounters.add(group);
          } else {
            // save this position for us to check in the next iteration
            matchesOnThisSequence.put(offset, counter);
          }
        } else {
          // check for each nearby position from the first iteration
          int searchStart, searchEnd;
          // Reversing the query should reverse the direction of the search
          // Reversing the order in which we check query components should reverse the order of the search
          // When the qury match is reverse and we're iterating in forward order, maxOffsetBetweenComponents should be added
          boolean otherSequenceExpectEarlier = (queryMatchReversed == lastComponentIsLargest);
          if (otherSequenceExpectEarlier) {
            searchStart = offset - maxReverseOffset;
            searchEnd = offset + this.maxOffsetBetweenComponents;
          } else {
            searchStart = offset - this.maxOffsetBetweenComponents;
            searchEnd = offset + maxReverseOffset;
          }

          // find nearby entries
          NavigableMap<Integer, HashBlockMatch_Counter> nearbyEntries = matchesOnThisSequence.subMap(searchStart, true, searchEnd, true);
          if (queryMatchReversed && nearbyEntries.size() > 1) {
            nearbyEntries = nearbyEntries.descendingMap();
          }

          for (Map.Entry<Integer, HashBlockMatch_Counter> nearbyEntry: nearbyEntries.entrySet()) {
            List<HashBlockMatch_Counter> matchingCounters = new ArrayList<HashBlockMatch_Counter>(2);
            if (lastComponentIsLargest) {
              matchingCounters.add(nearbyEntry.getValue());
              matchingCounters.add(counter);
            } else {
              matchingCounters.add(counter);
              matchingCounters.add(nearbyEntry.getValue());
            }
            matchedCounters.add(matchingCounters);
          }
        }
      }
    }

    // now collect the results where each component had a match
    List<QueryMatch> results = assembleQueryMatches(matchedCounters);
    results.addAll(guessedMatches);
    return results;
  }

  private List<QueryMatch> assembleQueryMatches(List<List<HashBlockMatch_Counter>> matchingComponents) {
    List<QueryMatch> results = new ArrayList<QueryMatch>();

    for (List<HashBlockMatch_Counter> group: matchingComponents) {
      List<SequenceMatch> sequenceMatches = matchCountersToSequenceMatches(group);
      boolean hintSearchForward;
      if (group.size() > 1) {
        hintSearchForward = group.get(0).getNumDistinctMismatches() < group.get(1).getNumDistinctMismatches();
      } else {
        hintSearchForward = true;
      }
      int numMismatches = countPriority(group);
      results.add(new QueryMatch(sequenceMatches, numMismatches, hintSearchForward));
    }

    return results;
  }

  private List<QueryMatch> filterMatchesHavingPriority(List<QueryMatch> matches, int numDistinctMismatches) {
    int numEarlierPriority = 0;
    int numLaterPriority = 0;
    List<QueryMatch> results = new ArrayList<QueryMatch>();
    for (QueryMatch match: matches) {
      int priority = match.getPriority();
      if (priority == numDistinctMismatches) {
        results.add(match);
      } else {
        if (priority < numDistinctMismatches) {
          numEarlierPriority++;
        } else {
          numLaterPriority++;
        }
      }
    }
    if (this.logger.getEnabled()) {
      String message = "filterMatchesHavingPriority checked " + matches.size() + " candidates for priority " + numDistinctMismatches + ". ";
      if (numEarlierPriority > 0)
        message += "Found " + numEarlierPriority + " matches that should have already been processed. ";
      if (results.size() > 0)
	message += "Found " + results.size() + " matches with priority " + numDistinctMismatches + ". ";
      if (numLaterPriority > 0)
        message += "Found " + numLaterPriority + " matches to process later.";
      this.logger.log(message);
    }
    return results;
  }

  private List<QueryMatch> filterMatchesHavingMinPriority(List<QueryMatch> matches) {
    int min = -1;
    for (QueryMatch match: matches) {
      if (min < 0 || min < match.getPriority()) {
        min = match.getPriority();
      }
    }
    return this.filterMatchesHavingPriority(matches, min);
  }

  private List<SequenceMatch> matchCountersToSequenceMatches(List<HashBlockMatch_Counter> counters) {
    List<SequenceMatch> matches = new ArrayList<SequenceMatch>(counters.size());
    for (HashBlockMatch_Counter counter: counters) {
      matches.add(counter.getMatch());
    }
    return matches;
  }

  private int countPriority(List<HashBlockMatch_Counter> counters) {
    if (counters.size() == 2) {
      SequenceMatch match1 = counters.get(0).getMatch();
      SequenceMatch match2 = counters.get(1).getMatch();
      if (match1.getStartIndexB() < match2.getEndIndexB() && match1.getEndIndexB() > match2.getStartIndexB()) {
        // If any sequence matches overlap, then any hashblock mismatches might be counted multiple times
        // We want to compute the priority of the query match, so we need a lower bound on the deduplicated mismatches
        int max = 0;
        for (HashBlockMatch_Counter counter: counters) {
          max = Math.max(max, counter.getPriority());
        }
        return max;
      }
    }
    // If the matches don't overlap, then each mismatch is distinct
    int total = 0;
    for (HashBlockMatch_Counter counter: counters) {
      total += counter.getPriority();
    }
    return total;
  }

  private List<Counting_HashBlockPath> components;
  private int maxOffsetBetweenComponents;
  private int expectedOffsetBetweenComponents;
  private Logger logger;
  private int lastNumMatchesPrinted = 0;
  private List<QueryMatch> previousAssembledMatches;
  private List<List<HashBlockMatch_Counter>> previousMatchComponents;
  private boolean foundNonemptyResult;
}
