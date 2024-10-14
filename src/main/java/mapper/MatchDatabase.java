package mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Stores a collection of alignedBlocks
public class MatchDatabase implements AlignmentListener {
  public MatchDatabase(double queryEndFraction) {
    this.queryEndFraction = queryEndFraction;
    this.alignmentsBySequence = new HashMap<Sequence, Alignments>();
  }

  // Adds the given QueryAlignment's to this
  public void addAlignments(List<List<QueryAlignment>> alignments) {
    Map<Sequence, List<WeightedAlignment>> alignmentsByReference = this.groupByReference(alignments);

    List<Alignments> recipients = new ArrayList<Alignments>();
    for (Map.Entry<Sequence, List<WeightedAlignment>> job: alignmentsByReference.entrySet()) {
      Sequence reference = job.getKey();
      Alignments alignmentsHere;
      synchronized (this.alignmentsBySequence) {
        alignmentsHere = this.getOrCreateAlignments(reference);
      }
      recipients.add(alignmentsHere);
      alignmentsHere.add(job.getValue());
    }
    for (Alignments recipient: recipients) {
      recipient.offerProcess();
    }
  }

  public void addUnaligned(List<Query> unalignedQueries) {
  }

  private Map<Sequence, List<WeightedAlignment>> groupByReference(List<List<QueryAlignment>> alignments) {
    Map<Sequence, List<WeightedAlignment>> alignmentsByReference = new HashMap<Sequence, List<WeightedAlignment>>();

    for (List<QueryAlignment> queryAlignments: alignments) {
      float weight = (float)1.0 / (float)queryAlignments.size();
      for (QueryAlignment queryAlignment: queryAlignments) {
        for (SequenceAlignment alignment: queryAlignment.getComponents()) {
          List<AlignedBlock> blocks = alignment.getSections();
          if (blocks.size() > 0) {
            Sequence reference = blocks.get(0).getSequenceB();
            List<WeightedAlignment> alignmentsOnThisRef = alignmentsByReference.get(reference);
            if (alignmentsOnThisRef == null) {
              alignmentsOnThisRef = new ArrayList<WeightedAlignment>();
              alignmentsByReference.put(reference, alignmentsOnThisRef);
            }
            alignmentsOnThisRef.add(new WeightedAlignment(alignment, queryAlignment, weight));
          }
        }
      }
    }
    return alignmentsByReference;
  }


  // Map from name of sequence to Alignments on that sequence
  public Map<Sequence, Alignments> groupByPosition() {
    return alignmentsBySequence;
  }

  private Alignments getOrCreateAlignments(Sequence reference) {
    Alignments alignments = alignmentsBySequence.get(reference);
    if (alignments == null) {
      alignments = new Alignments(reference, this.queryEndFraction);
      alignmentsBySequence.put(reference, alignments);
    }
    return alignments;
  }

  private Map<Sequence, Alignments> alignmentsBySequence;
  private double queryEndFraction;
}
