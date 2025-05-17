package mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// An Alignments tells how various Sequences align to a particular reference contig
public class Alignments {
  private static final int positionsPerSection = 65536;

  public Alignments(Sequence sequence, double queryEndFraction) {
    this.sequence = sequence;
    this.queryEndFraction = queryEndFraction;
    this.sections = new AlignmentsSection[sequence.getLength() / positionsPerSection + 1];
  }

  // Adds a pending job to add this alignment to this
  public void add(List<WeightedAlignment> alignments) {
    synchronized(this.pendingAdds) {
      this.pendingAdds.add(alignments);
    }
  }

  // Processes the pending jobs if no other thread is currently processing them
  public void offerProcess() {
    synchronized(this.pendingAdds) {
      if (this.activelyAdding) {
        // there's already another thread processing jobs
        return;
      }
      this.activelyAdding = true;
    }
    this.processAll();
  }

  // processes all jobs in this.pendingAdds until none remain unprocessed
  private void processAll() {
    while (true) {
      List<WeightedAlignment> job = null;
      synchronized(this.pendingAdds) {
        if (this.pendingAdds.size() > 0) {
          if (this.pendingAdds.size() > 32) {
            // If we get too many pending jobs, we block new jobs until existing jobs are done
            for (List<WeightedAlignment> j : this.pendingAdds) {
              this.process(j);
            }
            this.pendingAdds.clear();
            this.activelyAdding = false;
            return;
          }

          // Normally we just select the next job to process and then reopen the queue
          job = this.pendingAdds.get(this.pendingAdds.size() - 1);
          this.pendingAdds.remove(this.pendingAdds.size() - 1);
        } else {
          // done
          this.activelyAdding = false;
          return;
        }
      }
      this.process(job);
    }
  }


  private void process(List<WeightedAlignment> jobs) {
    for (WeightedAlignment weightedAlignment : jobs) {
      this.addAlignment(weightedAlignment);
    }
  }

  private void addAlignment(WeightedAlignment weightedAlignment) {
    try {
      for (AlignedBlock block : weightedAlignment.getAlignment().getSections()) {
        this.addMatchOnSequence(block, weightedAlignment);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed save alignment " + weightedAlignment.getQueryAlignment().formatQuery(), e);
    }
  }

  private void addMatchOnSequence(AlignedBlock block, WeightedAlignment weightedAlignment) {
    // add the bases in this block to the result
    //String textA;
    int referenceStartIndex;
    int queryStartIndex;
    Sequence sequenceA = block.getSequenceA();
    Sequence subsequenceA;
    subsequenceA = block.getSubsequenceA();
    referenceStartIndex = block.getStartIndexB();
    queryStartIndex = block.getStartIndexA();
    SequenceAlignment sequenceAlignment = weightedAlignment.getAlignment();
    boolean reverse = sequenceAlignment.isReferenceReversed();

    int queryFirstStartIndex = sequenceAlignment.getStartIndexA();
    int queryLastEndIndex = sequenceAlignment.getEndIndexA();

    if (block.aLength == block.bLength) {
      // a normal 1-to-1 mapping
      for (int i = 0; i < block.aLength; i++) {
        int referenceIndex = referenceStartIndex + i;
        int queryIndex = queryStartIndex + i;
        boolean nearQueryEnd = this.isNearQueryEnd(sequenceA, queryFirstStartIndex, queryLastEndIndex, queryIndex);
        float weight = weightedAlignment.getWeight(referenceIndex);
        byte encodedBaseHere = subsequenceA.encodedCharAt(i);
        int aIndex = block.getStartIndexA() + i;
        if (reverse) {
          this.addReverse(referenceIndex, encodedBaseHere, weight, sequenceA, aIndex, nearQueryEnd);
        } else {
          this.addForward(referenceIndex, encodedBaseHere, weight, sequenceA, aIndex, nearQueryEnd);
        }
      }
    } else {
      if (block.aLength > block.bLength) {
        // insertions
        String basesHere = subsequenceA.getRange(0, block.aLength);
        int referenceIndex = referenceStartIndex - 1;
        int queryIndex = queryStartIndex;
        boolean nearQueryEnd = this.isNearQueryEnd(sequenceA, queryFirstStartIndex, queryLastEndIndex, queryIndex);
        float weight = weightedAlignment.getWeight(referenceIndex);
        if (reverse) {
          this.insertReverse(referenceIndex, basesHere, weight, sequenceA, block.getStartIndexA(), nearQueryEnd);
        } else {
          this.insertForward(referenceIndex, basesHere, weight, sequenceA, block.getStartIndexA(), nearQueryEnd);
        }
      } else {
        // deletions
        byte dash = Basepairs.encode('-');
        for (int i = 0; i < block.bLength; i++) {
          int referenceIndex = referenceStartIndex + i;
          int queryIndex = queryStartIndex + i;
          boolean nearQueryEnd = this.isNearQueryEnd(sequenceA, queryFirstStartIndex, queryLastEndIndex, queryIndex);
          float weight = weightedAlignment.getWeight(referenceIndex);
          int aIndex = block.getStartIndexA();
          if (reverse) {
            this.addReverse(referenceIndex, dash, weight, sequenceA, aIndex, nearQueryEnd);
          } else {
            this.addForward(referenceIndex, dash, weight, sequenceA, aIndex, nearQueryEnd);
          }
        }
      }
    }
  }


  boolean isNearQueryEnd(Sequence sequence, int queryAlignmentFirstStartIndex, int queryAlignmentLastEndIndex, int queryIndex) {
    int distance = Math.min(queryIndex - queryAlignmentFirstStartIndex, queryAlignmentLastEndIndex - queryIndex - 1);
    return distance < sequence.getLength() * this.queryEndFraction;
  }

  private void addForward(int referenceIndex, byte encodedValue, float weight, Sequence querySequence, int queryPosition, boolean nearQueryEnd) {
    AlignmentsSection section = getOrCreateSection(referenceIndex);
    int index = getIndexOfSection(referenceIndex);
    section.addForward(index, encodedValue, weight, querySequence, queryPosition, nearQueryEnd);
  }
  private void addReverse(int referenceIndex, byte encodedValue, float weight, Sequence querySequence, int queryPosition, boolean nearQueryEnd) {
    AlignmentsSection section = getOrCreateSection(referenceIndex);
    int index = getIndexOfSection(referenceIndex);
    section.addReverse(index, encodedValue, weight, querySequence, queryPosition, nearQueryEnd);
  }
  private void insertForward(int referenceIndex, String value, float weight, Sequence querySequence, int queryPosition, boolean nearQueryEnd) {
    AlignmentsSection section = getOrCreateSection(referenceIndex);
    int index = getIndexOfSection(referenceIndex);
    section.insertForward(index, value, weight, querySequence, queryPosition, nearQueryEnd);
  }
  private void insertReverse(int referenceIndex, String value, float weight, Sequence querySequence, int queryPosition, boolean nearQueryEnd) {
    AlignmentsSection section = getOrCreateSection(referenceIndex);
    int index = getIndexOfSection(referenceIndex);
    section.insertReverse(index, value, weight, querySequence, queryPosition, nearQueryEnd);
  }

  public int size() {
    return this.sequence.getLength();
  }

  public AlignmentPosition getPosition(int referenceIndex) {
    AlignmentsSection section = getSection(referenceIndex);
    if (section == null) {
      return new AlignmentPosition(this.sequence.charAt(referenceIndex));
    }
    int index = getIndexOfSection(referenceIndex);
    return section.getPosition(index);
  }

  public AlignmentPosition getInsertion(int referenceIndex, int insertionIndex) {
    AlignmentsSection section = getOrCreateSection(referenceIndex);
    int index = getIndexOfSection(referenceIndex);

    return section.getInsertion(index, insertionIndex);
  }

  public Sequence getSequence() {
    return this.sequence;
  }

  public double getQueryEndFraction() {
    return this.queryEndFraction;
  }

  private AlignmentsSection getSection(int referencePosition) {
    return this.sections[referencePosition / positionsPerSection];
  }

  private AlignmentsSection getOrCreateSection(int referencePosition) {
    int index = referencePosition / positionsPerSection;
    AlignmentsSection section = this.sections[index];
    if (section == null) {
      int startIndex = index * positionsPerSection;
      int endIndex = Math.min(startIndex + positionsPerSection, this.sequence.getLength());
      section = new AlignmentsSection(new Subsequence(this.sequence, startIndex, endIndex - startIndex));
      this.sections[index] = section;
    }
    return section;
  }

  private int getIndexOfSection(int referencePosition) {
    return referencePosition - (referencePosition / positionsPerSection * positionsPerSection);
  }


  private Sequence sequence;
  private double queryEndFraction;
  private AlignmentsSection[] sections;
  private static char[] allBases = new char[]{'A', 'C', 'G', 'T'};

  private List<List<WeightedAlignment>> pendingAdds = new ArrayList<List<WeightedAlignment>>();
  private boolean activelyAdding = false;
}
