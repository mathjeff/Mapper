package mapper;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

// The Api class contains functions that are intended to be called directly by code from other projects
// We will try not to make incompatible changes to functions in this file too often

// For usage examples, see ApiTest.java

public class Api {

  // This function aligns one query to one reference
  // It's less efficient to call this function lots of times than it is give all of the work to an AlignerWorker at once (because that reuses the analyses: HashBlock_Database and AlignmentCache)
  public static List<QueryAlignment> alignOnce(String queryText, String referenceText, AlignmentParameters parameters, Logger logger) {
    Sequence querySequence = new SequenceBuilder().setName("query").add(queryText).build();
    Query query = new Query(querySequence);
    return alignOnce(query, referenceText, parameters, logger);
  }

  // This function aligns one query to one reference
  // It's less efficient to call this function lots of times than it is give all of the work to an AlignerWorker at once (because that reuses the analyses: HashBlock_Database and AlignmentCache)
  public static List<QueryAlignment> alignOnce(Query query, String referenceText, AlignmentParameters parameters, Logger logger) {
    long startMillis = System.currentTimeMillis();
    Sequence reference = new SequenceBuilder().setName("reference").add(referenceText).build();
    List<Sequence> references = new ArrayList<Sequence>();
    references.add(reference);
    references.add(reference.reverseComplement());
    SequenceDatabase refSequences = new SequenceDatabase(references);
    StatusLogger statusLogger = new StatusLogger(logger, startMillis);
    HashBlock_Database hashblockDatabase = new HashBlock_Database(refSequences, statusLogger);
    List<AlignmentListener> alignmentListeners = new ArrayList<AlignmentListener>();
    AlignmentCache resultsCache = new AlignmentCache();

    int minDuplicationLength = DuplicationDetector.chooseMinDuplicationLength(refSequences);
    int maxDuplicationLength = DuplicationDetector.chooseMaxDuplicationLength(refSequences);
    DuplicationDetector duplicationDetector = new DuplicationDetector(hashblockDatabase, minDuplicationLength, maxDuplicationLength, 2, 1, null, statusLogger);

    AlignerWorker worker = new AlignerWorker(hashblockDatabase, parameters, duplicationDetector.getView(logger), 0, alignmentListeners, resultsCache, new ArrayDeque<AlignerWorker>());
    worker.setup();
    worker.setLogger(logger);
    return worker.align(query).getAlignmentsForQuery(query);
  }

}
