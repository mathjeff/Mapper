package mapper;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

// A SamWriter writes .sam files
// See https://samtools.github.io/hts-specs/SAMv1.pdf for more information
public class SamWriter implements AlignmentListener {
  public SamWriter(SequenceDatabase sequenceDatabase, String path, boolean explainPairedEndReads) throws FileNotFoundException {
    File file = new File(path);
    this.fileStream = new FileOutputStream(file);
    this.bufferedStream = new BufferedOutputStream(fileStream);
    // write header
    this.writeComment("Sequence Alignment Map");
    this.writeComment("Format version, sort order");
    this.write("@HD\tVN:1.6\tSO:unsorted\n");
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
    this.write("@PG\tID:mapper\tPN:mapper\tVN:" + version + "\tCL:\"" + invocation + "\"\n");
  }

  private void writeReferenceSequenceNames(SequenceDatabase sequenceDatabase) {
    int count = sequenceDatabase.getNumSequences();
    this.writeComment("Each contig in the reference genome (one per line):");
    for (int i = 0; i < count; i++) {
      Sequence contig = sequenceDatabase.getSequence(i);
      if (contig.getComplementedFrom() == null)
        this.write("@SQ\tSN:" + contig.getName() + "\tLN:" + contig.getLength() + "\n");
    }
  }

  private void explainAlignmentFormat(boolean explainPairedEndReads) {
    this.writeComment("Format of a query alignment (one line per sequence):");
    StringBuilder formatCommentBuilder = new StringBuilder();
    formatCommentBuilder.append(" Query name, flags (direction), reference contig, position, mapping quality (unused), CIGAR (indels), ");
    if (explainPairedEndReads) {
      formatCommentBuilder.append("mate reference name, mate reference position, ");
    } else {
      formatCommentBuilder.append("mate reference name (unused), mate reference position (unused), ");
    }
    formatCommentBuilder.append("query length, query sequence, quality (unused), ");
    if (explainPairedEndReads) {
      formatCommentBuilder.append("more (combined alignment score, aligment score)");
    } else {
      formatCommentBuilder.append("more (aligment score)");
    }
    this.writeComment(formatCommentBuilder.toString());
    this.writeComment("");
    this.writeComment("Alignment score format:");
    if (explainPairedEndReads) {
      this.writeComment(" CAS:f:<float>   combined alignment score of this query and its mate = <float>");
      this.writeComment("  Combined alignment score (CAS) = (mate1.score + mate2.score - overlap.score) * (mate1.length + mate2.length) / (unique length) + spacing.score . See --verbose output for more details.");
      this.writeComment("");
    }
    this.writeComment(" AS:f:<float>    score of alignment = <float>");
    this.writeComment("");
  }

  public void addAlignments(List<List<QueryAlignment>> alignments) {
    this.writeAndFlush(this.format(alignments));
  }

  public void addUnaligned(List<Query> unalignedQueries) {
  }

  public void close() {
    try {
      this.bufferedStream.close();
    } catch (IOException e) {
    }
    try {
      this.fileStream.close();
    } catch (IOException e) {
    }
  }

  private String format(List<List<QueryAlignment>> alignments) {
    StringBuilder builder = new StringBuilder();
    for (List<QueryAlignment> queryAlignments: alignments) {
      for (QueryAlignment queryAlignment: queryAlignments) {
        try {
          formatQueryAlignment(queryAlignment, builder);
        } catch (Exception e) {
          throw new RuntimeException("Failed to write alignment for " + queryAlignment.formatQuery(), e);
        }
      }
    }
    return builder.toString();
  }

  private void formatQueryAlignment(QueryAlignment queryAlignment, StringBuilder builder) {
     String queryPenaltyFormatted = null;
     if (queryAlignment.getNumSequences() > 1) {
       queryPenaltyFormatted = formatQueryPenalty(queryAlignment);
     }
     for (SequenceAlignment alignment: queryAlignment.getComponents()) {
       Sequence query = alignment.getSequenceA();
       Sequence ref = alignment.getSequenceB();
       // QNAME
       builder.append(query.getSourceName());
       builder.append('\t');
       // FLAG
       builder.append("" + getSamFlags(alignment));
       builder.append('\t');
       // RNAME
       builder.append(ref.getName());
       builder.append('\t');
       // POS
       builder.append(getSamReferencePosition(alignment));
       builder.append('\t');
       // MAPQ
       builder.append("255\t");
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
       SequenceAlignment pairedSequenceAlignment = getPaired(queryAlignment, alignment);
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
       if (queryPenaltyFormatted != null) {
         builder.append(queryPenaltyFormatted);
         builder.append("\t");
       }
       builder.append(formatSequencePenalty(alignment));
       builder.append("\n");
    }
  }

  private String formatSequencePenalty(SequenceAlignment alignment) {
    float score = (float)(-1 * alignment.getPenalty());
    return "AS:" + formatNumber(score);
  }

  private String formatQueryPenalty(QueryAlignment alignment) {
    float score = (float)(-1 * alignment.getPenalty());
    return "CAS:" + formatNumber(score);
  }

  private String formatNumber(double number) {
    float scale = 10000;
    float roundedNumber = Math.round(number * scale) / scale;
    return "f:" + roundedNumber;
  }

  private int getSamFlags(SequenceAlignment alignment) {
    int flags = 0;
    if (alignment.isReferenceReversed()) {
      flags += 16;
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
    this.write("@CO " + comment + "\n");
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
      this.bufferedStream.write(block);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  FileOutputStream fileStream;
  BufferedOutputStream bufferedStream;
  Queue<byte[]> pendingWrites = new ArrayDeque<byte[]>();
  boolean activelyWriting = false;
}
