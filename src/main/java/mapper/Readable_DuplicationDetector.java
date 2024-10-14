package mapper;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

// detects duplication in a genome
public class Readable_DuplicationDetector {
  public Readable_DuplicationDetector(DuplicationDetector source, Logger logger) {
    this.duplicationDetector = source;
    this.logger = logger;
  }

  // Returns interesting duplications on this sequence
  // A duplication is considered interesting if it doesn't contain another duplication
  public TreeMap<Integer, Duplication> getInterestingDuplicationsOnSequence(Sequence sequence) {
    this.ensureSetup();
    TreeMap<Integer, Duplication> duplicationsHere = this.interestingDuplicationsBySequence.get(sequence);
    return duplicationsHere;
  }

  // Determines whether it's possible for there to be an interesting duplication in this range
  // If so, returns a non-null Duplication
  // Usually, the returned Duplication will be in the given range, but not necessarily
  // (It's possible that we didn't save the Duplication that is in this range)
  public Duplication mayContainDuplicationInRange(Sequence sequence, int startIndex, int endIndex) {
    int windowStart = duplicationDetector.getWindowNumber(startIndex);
    int windowEnd = duplicationDetector.getWindowNumber(endIndex);
    TreeMap<Integer, Duplication> entriesHere = getInterestingDuplicationsOnSequence(sequence);
    if (entriesHere == null)
      return null; // no duplications on this sequence
    Map.Entry<Integer, Duplication> previous = entriesHere.floorEntry(endIndex);
    if (previous != null) {
      int previousWindow = duplicationDetector.getWindowNumber(previous.getKey());
      if (previousWindow >= windowStart && previousWindow <= windowEnd)
        return previous.getValue();
    }
    Map.Entry<Integer, Duplication> next = entriesHere.ceilingEntry(startIndex);
    if (next != null) {
      int nextWindow = duplicationDetector.getWindowNumber(next.getKey());
      if (nextWindow >= windowStart && nextWindow <= windowEnd)
        return next.getValue();
    }
    return null;
  }

  // Returns all detected duplications
  // Some duplications might also contain others
  // To filter out positions where a Duplication contains other duplications, use getInterestingDuplicationsOnSequence
  // Note that even if one position of a Duplication container another Duplication, other positions of that duplication might not, if gapmers are enabled (and they're enabled by default)
  public Set<Duplication> getAll() {
    this.ensureSetup();
    return this.allDuplications;
  }

  public double getDetectionGranularity() {
    return duplicationDetector.getDetectionGranularity();
  }

  public void helpSetup() {
    this.ensureSetup();
  }

  private void ensureSetup() {
    if (this.interestingDuplicationsBySequence == null) {
      this.duplicationDetector.setup(this, this.logger);
    }
  }

  public void setup(Map<Sequence, TreeMap<Integer, Duplication>> interestingDuplicationsBySequence, Set<Duplication> allDuplications) {
    this.interestingDuplicationsBySequence = interestingDuplicationsBySequence;
    this.allDuplications = allDuplications;
  }

  DuplicationDetector duplicationDetector;
  Map<Sequence, TreeMap<Integer, Duplication>> interestingDuplicationsBySequence;
  Set<Duplication> allDuplications;
  Logger logger;
}
