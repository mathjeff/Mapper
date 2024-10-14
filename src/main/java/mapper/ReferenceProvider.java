package mapper;

interface ReferenceProvider {
  HashBlock_Database get_HashBlock_database(Logger logger);
  boolean getCanUseHelp();
  Sequence getOriginalSequence(Sequence modified);
  boolean getEnableGapmers();
}
