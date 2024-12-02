package mapper;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;

// A SamWriter writes .sam files
// See https://samtools.github.io/hts-specs/SAMv1.pdf for more information
public class SamWriter implements AlignmentListener {
  public SamWriter(SequenceDatabase sequenceDatabase, OutputStream outputStream, boolean explainPairedEndReads) {
    this.outputStream = outputStream;
    // write header
    this.writeComment("Sequence Alignment Map");
    this.writeComment("Format version, group order");
    this.writeLine("@HD\tVN:1.6\tGO:query");
    this.writeComment("");
    this.writeComment("Invocation details (approximate)");
    this.writeInvocationDetails();
    this.writeComment("");
    // write reference sequence names
    this.writeReferenceSequenceNames(sequenceDatabase);
    // explain the alignment format
    this.writeComment("");
    this.explainAlignmentFormat(explainPairedEndReads);
    this.flush();
  }

  private void writeInvocationDetails() {
    String version = MapperMetadata.getVersion();
    String invocation = MapperMetadata.guessCommandLine();
    this.writeLine("@PG\tID:mapper\tPN:mapper\tVN:" + version + "\tCL:\"" + invocation + "\"");
  }

  private void writeReferenceSequenceNames(SequenceDatabase sequenceDatabase) {
    int count = sequenceDatabase.getNumSequences();
    this.writeComment("Each contig in the reference genome (one per line):");
    for (int i = 0; i < count; i++) {
      Sequence contig = sequenceDatabase.getSequence(i);
      if (contig.getComplementedFrom() == null)
        this.writeLine("@SQ\tSN:" + contig.getName() + "\tLN:" + contig.getLength());
    }
  }

  private void explainAlignmentFormat(boolean explainPairedEndReads) {
    this.writeComment("Format of a query alignment (one line per sequence):");
    StringBuilder formatCommentBuilder = new StringBuilder();
    formatCommentBuilder.append(" Query name, SAM flags, reference contig, position, mapping quality, CIGAR (indels), ");
    if (explainPairedEndReads) {
      formatCommentBuilder.append("mate reference name, mate reference position, ");
    } else {
      formatCommentBuilder.append("mate reference name (unused), mate reference position (unused), ");
    }
    formatCommentBuilder.append("query length, query sequence, nucleotide quality (unused), ");
    if (explainPairedEndReads) {
      formatCommentBuilder.append("more (combined alignment score, aligment score)");
    } else {
      formatCommentBuilder.append("more (aligment score)");
    }
    this.writeComment(formatCommentBuilder.toString());
    this.writeComment("");
    if (explainPairedEndReads) {
      this.writeComment("  SAM Flags is a bitwise-or of these values:");
    } else {
      this.writeComment("  SAM Flags:");
    }
    if (explainPairedEndReads) {
      this.writeComment("   1: This alignment comes from a paired-end read");
      this.writeComment("   2: This alignment involves a read and its mate aligned near each other");
      this.writeComment("   8: No alignment was found for this read mate's mate");
    }
    this.writeComment("   16: This read sequence is reverse-complemented relative to the reference");
    if (explainPairedEndReads) {
      this.writeComment("   32: This read's mate is reverse-complemented relative to the reference");
      this.writeComment("   64: This read mate is mate #1");
      this.writeComment("   128: This read mate is the last mate");
      this.writeComment("   256: A better alignment was found for this query");
    }
    this.writeComment("");
    this.writeComment(" Alignment quality:");
    this.writeComment("  Let P represent the probability that the alignment position does not correspond to the biologically closest ancestor.");
    this.writeComment("  Alignment quality is an estimate of -10 * log(P) / log(1), rounded to the nearest integer, or 255 if unavailable.");
    this.writeComment("  In this version of Mapper, we estimate this value using this model:");
    this.writeComment("   0 if another alignment was found for this query with less penalty than this one");
    this.writeComment("   255 otherwise");
    this.writeComment("");
    this.writeComment("  Alignment score format:");
    if (explainPairedEndReads) {
      this.writeComment("   cs:f:<float>   combined alignment score of this query and its mate = <float>");
      this.writeComment("    Combined alignment score (CAS) = (mate1.score + mate2.score - overlap.score) * (mate1.length + mate2.length) / (unique length) + spacing.score . See --verbose output for more details.");
      this.writeComment("");
    }
    this.writeComment("   AS:f:<float>    score of alignment = <float>");
    this.writeComment("");
  }

  public void addAlignments(List<QueryAlignments> alignments) {
    this.writeAndFlush(this.format(alignments));
  }

  private String format(List<QueryAlignments> alignments) {
    StringBuilder builder = new StringBuilder();
    for (QueryAlignments queryAlignments: alignments) {
      formatQueryAlignments(queryAlignments, builder);
    }
    return builder.toString();
  }

  // Given a list of alignments for a single query, adds them to the given StringBuilder
  private void formatQueryAlignments(QueryAlignments candidateAlignments, StringBuilder builder) {
    for (Map.Entry<Query, List<QueryAlignment>> subqueryAlignments: candidateAlignments.getAlignments().entrySet()) {
      formatQueryAlignments(subqueryAlignments.getKey(), subqueryAlignments.getValue(), candidateAlignments, builder);
    }
  }

  private void formatQueryAlignments(Query subquery, List<QueryAlignment> subqueryAlignments, QueryAlignments queryAlignments, StringBuilder builder) {
    double minPenalty = Integer.MAX_VALUE;
    for (QueryAlignment alignment: subqueryAlignments) {
      minPenalty = Math.min(minPenalty, alignment.getPenalty());
    }
    for (QueryAlignment queryAlignment: subqueryAlignments) {
      boolean hasMinimumPenalty = queryAlignment.getPenalty() == minPenalty;
      try {
        formatQueryAlignment(queryAlignment, queryAlignments, hasMinimumPenalty, builder);
      } catch (Exception e) {
        throw new RuntimeException("Failed to write alignment for " + queryAlignment.formatQuery(), e);
      }
    }
  }

  private void formatQueryAlignment(QueryAlignment subqueryAlignment, QueryAlignments queryAlignments, boolean hasMinimumPenalty, StringBuilder builder) {
     String subqueryPenaltyFormatted = null;
     if (subqueryAlignment.getNumSequences() > 1) {
       subqueryPenaltyFormatted = formatQueryPenalty(subqueryAlignment);
     }
     for (SequenceAlignment alignment: subqueryAlignment.getComponents()) {
       Sequence query = alignment.getSequenceA();
       Sequence ref = alignment.getSequenceB();
       // QNAME
       builder.append(query.getSourceName());
       builder.append('\t');
       // FLAG
       builder.append("" + getSamFlags(alignment, subqueryAlignment, queryAlignments, hasMinimumPenalty));
       builder.append('\t');
       // RNAME
       builder.append(ref.getName());
       builder.append('\t');
       // POS
       builder.append(getSamReferencePosition(alignment));
       builder.append('\t');
       // MAPQ
       int mappingQuality = getMappingQuality(hasMinimumPenalty);
       builder.append("" + mappingQuality + "\t");
       // CIGAR flags
       int queryLengthConsumed = 0;
       for (AlignedBlock block : alignment.getSections()) {
         if (block.getStartIndexA() != queryLengthConsumed) {
           if (queryLengthConsumed != 0) {
             throw new IllegalArgumentException("Failed to write alignment for " + query.getName() + " with text " + query.getText() + ": previous block ended at query index " + queryLengthConsumed + " and next block starts at " + block.getStartIndexA());
           }
           // left side of the query fell off the reference
           builder.append("" + block.getStartIndexA() + "S");
           queryLengthConsumed = block.getStartIndexA();
         }
         if (block.getLengthA() == block.getLengthB()) {
           builder.append("" + block.getLengthA() + "M");
         } else {
           if (block.getLengthA() > block.getLengthB()) {
             builder.append("" + block.getLengthA() + "I");
           } else {
             builder.append("" + block.getLengthB() + "D");
           }
         }
         queryLengthConsumed += block.getLengthA();
       }
       if (queryLengthConsumed < query.getLength()) {
         // right side of the query fell off the reference
         builder.append("" + (query.getLength() - queryLengthConsumed) + "S");
       }
       builder.append('\t');
       SequenceAlignment pairedSequenceAlignment = getPaired(subqueryAlignment, alignment);
       if (pairedSequenceAlignment != null) {
         // RNEXT:
         builder.append(pairedSequenceAlignment.getSequenceB().getName());
         builder.append('\t');
         // PNEXT:
         builder.append(getSamReferencePosition(pairedSequenceAlignment));
         builder.append('\t');
       } else {
         // RNEXT:
         builder.append("*\t");
         // PNEXT:
         builder.append("0\t");
       }
       // TLEN
       builder.append("" + query.getLength());
       builder.append('\t');
       // SEQ
       builder.append(query.getText());
       builder.append('\t');
       // QUAL
       builder.append("*\t");
       // alignment score
       if (subqueryPenaltyFormatted != null) {
         builder.append(subqueryPenaltyFormatted);
         builder.append("\t");
       }
       builder.append(formatSequencePenalty(alignment));
       builder.append("\n");
    }
  }

  // This function provides a simple model for calculating MAPQ.
  //
  // Mapping quality is defined by the SAM spec as:
  //  -10 * log(Pr{mapping position is wrong}) / log(10), rounded to the nearest integer,
  //  or 255 if this value is not available.
  //
  // We interpret this to refer to the probability that the given alignment does not refer to the position having the biologically closest ancestor among possible alignment positions in the reference.
  //
  // Theoretically, an entity could keep a database of all organisms and determine the evolutionary history of a genome sequence from there, but making such a database is too difficult for us to do here, and does not seem to be the intent of the SAM spec.
  // The only data we have access to here is the set of query sequences and the reference genome.
  //
  // So, we further interpret this (the probability that the mapping position is wrong) to refer specifically to:
  //  the probability that the given alignment does not refer to the position having the biologically closest ancestor among possible alignment positions in the reference,
  //  as predicted by some model.
  //
  // It is too difficult for us to make a perfect model that correctly determines the evolutionary history of a given sequence based on that sequence and an arbitrary reference genome, particularly because organisms continue to evolve and the frequencies and probabilities of finding various sequences continue to change.
  //
  // However, we are told that making a prediction via a simple model may be more helpful to users than making no prediction, so we implement a simple model here.
  private int getMappingQuality(boolean hasMinimumPenalty) {
    if (!hasMinimumPenalty) {
      // This alignment has more penalty than another alignment that we found in this genome.
      // If we assume that lower-penalty alignments are at least as likely to represent the true biological history as higher-penalty alignments, then the probability that this alignment is incorrect is at least 1/2.
      //
      // In practice, if we examine some computed alignments for which we know the correct alignment, we observe the probability of the correct alignment to not be reported as having the minimum penalty is small: less than 10%.
      // So, we expect that the probability that the lowest-penalty alignment is not the correct alignment is less than 10%.
      // Then we expect that the probability that another alignment does not have the closest biological ancestor should be than 90%, which gives us a quality score of
      // round(-10 * log(0.9) / log(10)) = round(0.457...) = 0
      return 0;
    } else {
      // This alignment has the minimum penalty among alignments that we found for this read
      // However:
      // 1. We're not completely confident that there does not exist another alignment with lower penalty (sometimes we decide that an exhaustive search will take too long to be worth doing)
      // 2. Even if this alignment gives the lowest possible penalty among possible alignments for this query to this reference genome, we still don't know whether this alignment is to the closest descendant of its true biological ancestor in the reference. It's possible that more mutations occurred between the common ancestor and the current reference genome.
      //
      // For practical use we believe it's sufficient at the moment to report that the probability is unavailable (255)
      return 255;
    }
  }

  private String formatSequencePenalty(SequenceAlignment alignment) {
    float score = (float)(-1 * alignment.getPenalty());
    return "AS:" + formatNumber(score);
  }

  private String formatQueryPenalty(QueryAlignment alignment) {
    float score = (float)(-1 * alignment.getPenalty());
    return "cs:" + formatNumber(score);
  }

  private String formatNumber(double number) {
    float scale = 10000;
    float roundedNumber = Math.round(number * scale) / scale;
    return "f:" + roundedNumber;
  }

  // hasMinimumPenalty indicates whether the corresponding QueryAlignment has the minimum penalty among all discovered alignments for this query
  private int getSamFlags(SequenceAlignment sequenceAlignment, QueryAlignment subqueryAlignment, QueryAlignments alignments, boolean hasMinimumPenalty) {
    int flags = 0;

    // direction of alignment
    boolean reverseComplemented = sequenceAlignment.isReferenceReversed();
    if (reverseComplemented) {
      flags += 16;
    }

    // paired-end read information
    int thisAlignmentNumSequences = subqueryAlignment.getNumSequences();
    int numSubqueries = alignments.getNumQueries();
    boolean queryHasMultipleSequences = (thisAlignmentNumSequences > 1) || (numSubqueries > 1);
    if (queryHasMultipleSequences) {
      // query has a mate
      flags += 1;

      boolean matesAlignedTogether = (subqueryAlignment.getNumSequences() > 1);
      if (matesAlignedTogether) {
        // mates appear to be have taken from nearby locations on the same contig
        flags += 2;

        SequenceAlignment pairedAlignment = getPaired(subqueryAlignment, sequenceAlignment);
        boolean mateReverseComplemented = (pairedAlignment != null && pairedAlignment.isReferenceReversed());
        if (mateReverseComplemented) {
          flags += 32;
        }
      }

      boolean mateAligned = (matesAlignedTogether || alignments.getNumQueriesHavingAlignments() > 1);
      if (!mateAligned) {
        flags += 8;
      }

      // first or second mate
      Query firstSubquery = alignments.getFirstQuery();
      Sequence firstSubqueryFirstSequence = firstSubquery.getSequence(0);
      if (firstSubqueryFirstSequence.getComplementedFrom() != null)
        firstSubqueryFirstSequence = firstSubqueryFirstSequence.getComplementedFrom();
      Sequence thisQuerySequence = sequenceAlignment.getSequenceA();
      if (thisQuerySequence.getComplementedFrom() != null)
        thisQuerySequence = thisQuerySequence.getComplementedFrom();
      boolean isFirstMate = (firstSubqueryFirstSequence == thisQuerySequence);
      if (isFirstMate) {
        flags += 64;
      }
      boolean isLastMate = !isFirstMate;
      if (isLastMate) {
        flags += 128;
      }
    }

    // priority of alignment
    if (!hasMinimumPenalty) {
      flags += 256;
    }
    return flags;
  }

  // gets the other/paired SequenceAlignment in a QueryAlignment, or null if there is no other
  private SequenceAlignment getPaired(QueryAlignment queryAlignment, SequenceAlignment sequenceAlignment) {
    if (queryAlignment.getNumSequences() != 2)
      return null;
    List<SequenceAlignment> sequenceAlignments = queryAlignment.getComponents();
    if (sequenceAlignments.get(0) == sequenceAlignment)
      return sequenceAlignments.get(1);
    return sequenceAlignments.get(0);
  }

  private int getSamReferencePosition(SequenceAlignment sequenceAlignment) {
    return sequenceAlignment.getSection(0).getStartIndexB() + 1;
  }

  private void write(String text) {
    byte[] bytes = text.getBytes();
    synchronized(this.pendingWrites) {
      this.pendingWrites.add(bytes);
    }
  }

  private void writeLine(String text) {
    this.write(text + "\n");
  }

  private void writeAndFlush(String text) {
    byte[] bytes = text.getBytes();
    synchronized(this.pendingWrites) {
      this.pendingWrites.add(bytes);
      if (this.activelyWriting)
        return;
      this.activelyWriting = true;
    }
    this.flush();
  }

  private void writeComment(String comment) {
    this.writeLine("@CO\t" + comment);
  }

  private void flush() {
    while (true) {
      byte[] block;
      synchronized(this.pendingWrites) {
        if (this.pendingWrites.size() < 1) {
          this.activelyWriting = false;
          return;
        }
        if (this.pendingWrites.size() > 32) {
          // If we get too many pending jobs, we block new jobs until existing jobs are done) {
          for (byte[] currentBlock : this.pendingWrites) {
            this.sendToStream(currentBlock);
          }
          this.pendingWrites.clear();
          this.activelyWriting = false;
          return;
        }
        block = this.pendingWrites.remove();
      }
      this.sendToStream(block);
    }
  }

  private void sendToStream(byte[] block) {
    try {
      this.outputStream.write(block);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  OutputStream outputStream;
  Queue<byte[]> pendingWrites = new ArrayDeque<byte[]>();
  boolean activelyWriting = false;
}
