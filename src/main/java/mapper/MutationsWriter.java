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

// A MutationsWriter writes mutations observed in the queries compared to the reference
public class MutationsWriter {
  public MutationsWriter(String path, MutationDetectionParameters parameters) throws FileNotFoundException, IOException {
    this.initialize(new FileOutputStream(new File(path)));
    this.parameters = parameters;
  }

  public MutationsWriter(OutputStream destination, MutationDetectionParameters parameters) {
    this.initialize(destination);
    this.parameters = parameters;
  }

  private void initialize(OutputStream destination) {
    this.fileStream = destination;
    this.bufferedStream = new BufferedOutputStream(destination);
  }

  public void write(Map<Sequence, Alignments> alignments, int numParallelJobs) throws IOException {
    this.writeLine("# Version 1");
    this.writeLine("");
    this.writeLine("# Command line: \"" + MapperMetadata.guessCommandLine() + "\"");
    this.writeLine("");
    this.writeLine("# This file is a tab-separated table representing differences between a collection of aligned query sequences and reference sequences");
    this.writeLine("# CHROM refers to the name of the reference sequence");
    this.writeLine("# POS refers to the position of the mutation in the reference sequence");
    this.writeLine("# REF specifies the allele(s) of the reference in this position");
    this.writeLine("# ALT specifies the allele(s) of the query sequences in this position");
    this.writeLine("# DEPTH counts the total weight of query sequences supporting this mutation");
    this.writeLine("# TOTAL_DEPTH counts the total weight of query sequences aligned to the same position as this mutation");
    this.writeLine("# If a query has K possible alignments, it contributes 1/K weight to each position");
    this.writeLine("");
    this.writeLine("CHROM\tPOS\tREF\tALT\tDEPTH\tTOTAL_DEPTH");

    List<MutationsFormatRequest> jobs = this.splitJobs(alignments, numParallelJobs);
    int waitIndex = 0; // index of next worker to wait for
    List<MutationsFormatterWorker> workers = new ArrayList<MutationsFormatterWorker>();

    while (waitIndex < jobs.size()) {
      boolean hasCapacityToLaunchJob = (workers.size() < waitIndex + numParallelJobs);
      boolean hasJobToLaunch = workers.size() < jobs.size();
      if (hasCapacityToLaunchJob && hasJobToLaunch) {
        workers.add(requestFormat(jobs.get(workers.size())));
      } else {
        // cannot launch a new job; wait for one to complete instead
        MutationsFormatterWorker worker = workers.get(waitIndex);
        try {
          worker.join();
        } catch (InterruptedException e) {
        }
        // write results
        String result = worker.getResults();
        this.writeText(result);
        // clear worker
        workers.set(waitIndex, null);
        waitIndex++;
      }
    }

    bufferedStream.close();
    fileStream.close();
  }

  private List<MutationsFormatRequest> splitJobs(Map<Sequence, Alignments> alignments, int numParallelJobs) {
    List<MutationsFormatRequest> jobs = new ArrayList<MutationsFormatRequest>();

    TreeMap<String, Sequence> sortedSequences = new TreeMap<String, Sequence>();
    for (Map.Entry<Sequence, Alignments> entry : alignments.entrySet()) {
      sortedSequences.put(entry.getKey().getName(), entry.getKey());
    }
    int maxJobSize = 8192;

    for (Map.Entry<String, Sequence> entry : sortedSequences.entrySet()) {
      Sequence sequence = entry.getValue();
      Alignments alignmentsHere = alignments.get(sequence);
      FilteredAlignments filteredAlignments = new FilteredAlignments(alignmentsHere, this.parameters);
      int startIndex = 0;
      while (startIndex < sequence.getLength()) {
        int jobSize = Math.min(maxJobSize, Math.max(1, (jobs.size() + 1) / numParallelJobs * maxJobSize));

        int endIndex = Math.min(sequence.getLength(), startIndex + jobSize);
        int length = endIndex - startIndex;
        int jobId = jobs.size();
        jobs.add(new MutationsFormatRequest(sequence, startIndex, length, filteredAlignments, jobId));
        startIndex = endIndex;
      }
    }
    return jobs;
  }

  private MutationsFormatterWorker requestFormat(MutationsFormatRequest formatRequest) {
    MutationsFormatterWorker worker = new MutationsFormatterWorker();
    worker.request(formatRequest);
    worker.start();
    return worker;
  }

  private void writeText(String text) throws IOException {
    bufferedStream.write(text.getBytes());
  }
  private void writeLine(String line) throws IOException {
    this.writeText(line);
    this.writeText("\n");
  }

  BufferedOutputStream bufferedStream;
  OutputStream fileStream;
  MutationDetectionParameters parameters;
}
