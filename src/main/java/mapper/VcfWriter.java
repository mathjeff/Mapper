package mapper;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.TreeMap;

// A VcfWriter writes .vcf files
public class VcfWriter {
  public VcfWriter(String path, boolean includeNonMutations, MutationDetectionParameters mutationsFilter, double queryEndFraction, boolean showSupportRead) throws FileNotFoundException, IOException {
    this.initialize(new FileOutputStream(new File(path)));
    this.includeNonMutations = includeNonMutations;
    this.mutationsFilter = mutationsFilter;
    this.queryEndFraction = queryEndFraction;
    this.showSupportRead = showSupportRead;
  }

  public VcfWriter(OutputStream destination, boolean includeNonMutations) {
    this.initialize(destination);
    this.includeNonMutations = includeNonMutations;
  }

  private void initialize(OutputStream destination) {
    this.fileStream = destination;
    this.bufferedStream = new BufferedOutputStream(destination);
  }

  private double getQueryEndFraction(Map<Sequence, Alignments> alignments) {
    for (Alignments alignment: alignments.values()) {
      return alignment.getQueryEndFraction();
    }
    return 0;
  }

  public void write(Map<Sequence, Alignments> alignments, int numParallelJobs) throws IOException {
    double queryEndFraction = getQueryEndFraction(alignments);
    double queryEndPercent = queryEndFraction * 100;
    double queryMiddlePercent = 100 - queryEndPercent * 2;

    this.writeLine("##fileType=\"Vcf summary of variants\"");
    this.writeLine("");
    this.writeLine("##commandLine=\"" + MapperMetadata.guessCommandLine() + "\"");
    this.writeLine("");
    this.writeLine("### This file is a tab-separated table representing, for each position in a reference genome, the distribution of alleles in a set of aligned query sequences");
    this.writeLine("");
    this.writeLine("### CHROM refers to the name of the reference sequence");
    this.writeLine("### POS refers to the position in the reference sequence");
    this.writeLine("###   Negative numbers represent insertions");
    this.writeLine("### REF specifies the allele of the reference in this position");
    this.writeLine("### ALT specifies the alleles of the query sequences in this position");
    this.writeLine("### DP counts the total weight of query sequences at this position");
    this.writeLine("### DETAILS-MIDDLE counts the alleles from queries whose middle " + queryMiddlePercent + "% aligned to this position");
    this.writeLine("###   DETAILS-MIDDLE is a semicolon-delimited list of details about the weights for each allele listed in REF and ALT");
    this.writeLine("###   Each allele's details is displayed as the weight of forward reads supporting that allele, then a comma, then the weight of reverse reads supporting that allele");
    this.writeLine("###   So, the DETAILS-MIDDLE column may look like: REF-FWD,REF-REV;ALT1-FWD,ALT1-REV;ALT2-FWD,ALT2-REV...");
    this.writeLine("### DETAILS-ENDS counts the alleles from queries whose end " + queryEndPercent + "% aligned to this position");
    this.writeLine("###   DETAILS-ENDS is formatted in the same way as DETAILS-MIDDLE");
    this.writeLine("###   Note that weights reported in DETAILS-ENDS can be less informative because it is more difficult to distinguish an indel from a SNP near the end of a query sequence");
    this.writeLine("### If a query has K possible alignments, it contributes 1/K weight to each position");
    if (this.showSupportRead) {
      this.writeLine("### SUPPORT shows example queries for each variant, with the variant itself surrounded by brackets. If there are multiple variants, different supporting reads will be separated by commas");
    }
    this.writeLine("");

    if (this.showSupportRead) {
      this.writeText("#CHROM\tPOS\tREF\tALT\tDP\tDETAILS-MIDDLE\tDETAILS-ENDS\tSUPPORT\n");
    } else {
      this.writeText("#CHROM\tPOS\tREF\tALT\tDP\tDETAILS-MIDDLE\tDETAILS-ENDS\n");
    }

    List<VcfFormatRequest> jobs = this.splitJobs(alignments, numParallelJobs);
    int waitIndex = 0; // index of next worker to wait for
    List<VcfFormatterWorker> workers = new ArrayList<VcfFormatterWorker>();

    while (waitIndex < jobs.size()) {
      boolean hasCapacityToLaunchJob = (workers.size() < waitIndex + numParallelJobs);
      boolean hasJobToLaunch = workers.size() < jobs.size();
      if (hasCapacityToLaunchJob && hasJobToLaunch) {
        workers.add(requestFormat(jobs.get(workers.size())));
      } else {
        // cannot launch a new job; wait for one to complete instead
        VcfFormatterWorker worker = workers.get(waitIndex);
        try {
          worker.join();
        } catch (InterruptedException e) {
        }
        // write results
        String result = worker.getResults();
        this.writeText(result);
        this.numReferencePositionsMatched += worker.getNumReferencePositionsMatched();
        // clear worker
        workers.set(waitIndex, null);
        waitIndex++;
      }
    }

    bufferedStream.close();
    fileStream.close();
  }

  private List<VcfFormatRequest> splitJobs(Map<Sequence, Alignments> alignments, int numParallelJobs) {
    List<VcfFormatRequest> jobs = new ArrayList<VcfFormatRequest>();

    TreeMap<String, Sequence> sortedSequences = new TreeMap<String, Sequence>();
    for (Map.Entry<Sequence, Alignments> entry : alignments.entrySet()) {
      sortedSequences.put(entry.getKey().getName(), entry.getKey());
    }
    int maxJobSize = 8192;

    for (Map.Entry<String, Sequence> entry : sortedSequences.entrySet()) {
      Sequence sequence = entry.getValue();
      Alignments alignmentsHere = alignments.get(sequence);
      FilteredAlignments filteredAlignments = new FilteredAlignments(alignmentsHere, this.mutationsFilter);
      int startIndex = 0;
      while (startIndex < sequence.getLength()) {
        int jobSize = Math.min(maxJobSize, Math.max(1, (jobs.size() + 1) / numParallelJobs * maxJobSize));

        int endIndex = Math.min(sequence.getLength(), startIndex + jobSize);
        int length = endIndex - startIndex;
        int jobId = jobs.size();
        jobs.add(new VcfFormatRequest(sequence, startIndex, length, filteredAlignments, jobId));
        startIndex = endIndex;
      }
      this.numReferencePositions += sequence.getLength();
    }
    return jobs;
  }

  private VcfFormatterWorker requestFormat(VcfFormatRequest formatRequest) {
    VcfFormatterWorker worker = new VcfFormatterWorker(this.includeNonMutations, this.showSupportRead);
    worker.request(formatRequest);
    worker.start();
    return worker;
  }

  public long getNumReferencePositions() {
    return numReferencePositions;
  }
  public long getNumReferencePositionsMatched() {
    return numReferencePositionsMatched;
  }

  private void writeText(String text) throws IOException {
    bufferedStream.write(text.getBytes());
  }
  private void writeLine(String text) throws IOException {
    this.writeText(text);
    this.writeText("\n");
  }

  BufferedOutputStream bufferedStream;
  OutputStream fileStream;

  long numReferencePositionsMatched;
  long numReferencePositions;
  boolean includeNonMutations;
  double queryEndFraction;
  MutationDetectionParameters mutationsFilter;
  boolean showSupportRead;
}
