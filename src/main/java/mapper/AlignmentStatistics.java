package mapper;

public class AlignmentStatistics {

  long millisReadingQueries;
  long millisLaunchingWorkers;
  long millisWaitingForWorkers;

  long cpuMillisSpentOnUnalignedQueries;
  long cpuMillisSpentAligningMatches;
  long cpuMillisThroughOptimisticBestAlignments;

  Query slowestQuery;
  int slowestQueryNumAlignments;
  long slowestQueryMillis;

  Query queryAtRandomMoment;

  long numCasesImmediatelyAcceptingFirstAlignment;
  long numQueriesLoaded;
  long numCacheHits;

  long numIndels;
}
