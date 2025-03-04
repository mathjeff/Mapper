package mapper;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

// The Api class contains functions that are intended to be called directly by code from other projects
// We will try not to make incompatible changes to functions in this file too often

// For usage examples, see ApiTest.java

public class Api {

  // Constructs a ReferenceDatabase representing the given sequences (plus its reverse complement).
  public static ReferenceDatabase newDatabase(String reference, Logger logger) {
    List<String> references = new ArrayList<String>(1);
    references.add(reference);
    return newDatabase(references, logger);
  }

  // Constructs a ReferenceDatabase representing the given sequences (plus their reverse complements).
  public static ReferenceDatabase newDatabase(List<String> references, Logger logger) {
    long startMillis = System.currentTimeMillis();
    List<Sequence> referenceSequences = new ArrayList<Sequence>();
    for (int i = 0; i < references.size(); i++) {
      Sequence sequence = new SequenceBuilder().setName("reference-" + i).add(references.get(i)).build();
      if (sequence.getLength() < 1) {
        throw new RuntimeException("Sequence " + i + " has length " + sequence.getLength());
      }
      referenceSequences.add(sequence);
      referenceSequences.add(sequence.reverseComplement());
    }
    SequenceDatabase refSequences = new SequenceDatabase(referenceSequences);
    StatusLogger statusLogger = new StatusLogger(logger, startMillis);
    HashBlock_Database hashblockDatabase = new HashBlock_Database(refSequences, statusLogger);

    AlignmentCache resultsCache = new AlignmentCache();

    int minDuplicationLength = DuplicationDetector.chooseMinDuplicationLength(refSequences);
    int maxDuplicationLength = DuplicationDetector.chooseMaxDuplicationLength(refSequences);
    DuplicationDetector duplicationDetector = new DuplicationDetector(hashblockDatabase, minDuplicationLength, maxDuplicationLength, 2, 1, null, statusLogger);

    return new ReferenceDatabase(hashblockDatabase, duplicationDetector, resultsCache);
  }

  // Aligns a query string to a reference
  public static List<QueryAlignment> align(String query, ReferenceDatabase referenceDatabase, AlignmentParameters parameters, Logger logger) {
    Sequence querySequence = new SequenceBuilder().setName("query").add(query).build();
    return align(new Query(querySequence), referenceDatabase, parameters, logger);
  }

  // Aligns a query to a reference
  // Note that this function is expected to be threadsafe: it should be safe to call it in parallel from multiple threads, even with the same ReferenceDatabase
  public static List<QueryAlignment> align(Query query, ReferenceDatabase referenceDatabase, AlignmentParameters parameters, Logger logger) {
    HashBlock_Database hashblockDatabase = referenceDatabase.hashblockDatabase;
    DuplicationDetector duplicationDetector = referenceDatabase.duplicationDetector;
    AlignmentCache resultsCache = referenceDatabase.alignmentCache;
    List<AlignmentListener> alignmentListeners = new ArrayList<AlignmentListener>();

    AlignerWorker worker = new AlignerWorker(hashblockDatabase, parameters, duplicationDetector.getView(logger), 0, alignmentListeners, resultsCache, new ArrayDeque<AlignerWorker>());
    worker.setup();
    worker.setLogger(logger);
    return worker.align(query).getAlignmentsForQuery(query);
  }

  // This function aligns one query to one reference
  // It's less efficient to call this function lots of times than it is to call newDatabase() and give the database into align() each time
  public static List<QueryAlignment> alignOnce(String queryText, String referenceText, AlignmentParameters parameters, Logger logger) {
    Sequence querySequence = new SequenceBuilder().setName("query").add(queryText).build();
    Query query = new Query(querySequence);
    return alignOnce(query, referenceText, parameters, logger);
  }

  // This function aligns one query to one reference
  // It's less efficient to call this function lots of times than it is to call newDatabase() and give the database into align() each time
  public static List<QueryAlignment> alignOnce(Query query, String referenceText, AlignmentParameters parameters, Logger logger) {
    ReferenceDatabase referenceDatabase = newDatabase(referenceText, logger);
    return align(query, referenceDatabase, parameters, logger);
  }

}
