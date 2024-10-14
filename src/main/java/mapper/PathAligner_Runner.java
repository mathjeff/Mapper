package mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeSet;

// A PathAligner aligns two sequences by exploring possible alignments mostly along a path
public class PathAligner_Runner implements LocalAligner {
  public PathAligner_Runner() {
  }

  public void setLogger(Logger logger) {
    this.logger = logger;
  }

  public SequenceAlignment align(SequenceSection querySection, SequenceSection referenceSection, AlignmentParameters parameters, AlignmentAnalysis alignmentAnalysis) {
    // make a PathAligner_impl so this can be stateless
    return new PathAligner(logger).align(querySection, referenceSection, parameters, alignmentAnalysis);
  }
  private Logger logger;
}
