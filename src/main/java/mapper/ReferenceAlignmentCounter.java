package mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

// Count the number of reads aligned to each reference/genome
public class ReferenceAlignmentCounter implements AlignmentListener {
  public ReferenceAlignmentCounter() {
  }

  public void addAlignments(List<QueryAlignments> alignments) {
    synchronized (this) {
      for (QueryAlignments queryAlignments: alignments) {
        for (Map.Entry<Query, List<QueryAlignment>> alignment: queryAlignments.getAlignments().entrySet()) {
          List<QueryAlignment> subqueryAlignments = alignment.getValue();
          if (subqueryAlignments.size() > 0)
            this.addAlignmentsForQuery(subqueryAlignments);
        }
      }
    }
  }

  // Get reference path/filename of each reference sequence that mapped to this query
  private void addAlignmentsForQuery(List<QueryAlignment> alignments) {
    TreeSet<String> referenceList = new TreeSet<String>();
    for (QueryAlignment queryAlignment: alignments) {
      for (SequenceAlignment alignment: queryAlignment.getComponents()) {
        for (AlignedBlock block: alignment.getSections()) {
          referenceList.add(block.getSequenceB().getPath());
        }
      }
    }
    if (!this.referenceAlignmentCount.containsKey(referenceList)) {
      this.referenceAlignmentCount.put(referenceList, 0);
    }
    this.referenceAlignmentCount.put(referenceList, this.referenceAlignmentCount.get(referenceList) + 1);
  }
  // Summarize all references/genomes that mapped to this query
  public void sumAlignments(String outputPath) throws FileNotFoundException, IOException {
    File file = new File(outputPath);
    FileOutputStream fileStream = new FileOutputStream(file);
    BufferedOutputStream bufferedStream = new BufferedOutputStream(fileStream);
    bufferedStream.write("Genome_set\tNo.reads\n".getBytes());
    ArrayList<String> alloutput = new ArrayList<String>(referenceAlignmentCount.size());
    for (TreeSet<String> genomeset : referenceAlignmentCount.keySet()) {
      StringBuilder templine = new StringBuilder();
      for (String genome : genomeset){
        templine.append(splitPath(genome) + "-");
      }
      templine.append("\t" + referenceAlignmentCount.get(genomeset) + "\n");
      bufferedStream.write(templine.toString().getBytes());
    }
    bufferedStream.close();
    fileStream.close();
  }
  // Split path and get file filename
  public static String splitPath(String pathString) {
      return new File(pathString).getName();
  }
  private HashMap<TreeSet<String>,Integer> referenceAlignmentCount = new HashMap<TreeSet<String>,Integer>();//genome set, how many reads map to that genome set
}
