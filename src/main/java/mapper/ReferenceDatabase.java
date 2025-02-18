package mapper;

// A ReferenceDatabase stores analyses of a set of reference genomes
// A ReferenceDatabase can be created by callers in other projects via the Api class
public class ReferenceDatabase {

  // The functions in this class are not intended to be called by code from other projects.
  ReferenceDatabase(HashBlock_Database hashblockDatabase, DuplicationDetector duplicationDetector, AlignmentCache alignmentCache) {
    this.hashblockDatabase = hashblockDatabase;
    this.duplicationDetector = duplicationDetector;
    this.alignmentCache = alignmentCache;
  }

  HashBlock_Database hashblockDatabase;
  DuplicationDetector duplicationDetector;
  AlignmentCache alignmentCache;
}
