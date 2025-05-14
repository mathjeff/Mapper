package mapper;

import org.junit.Assert;
import org.junit.Test;

// The ApiTest class checks that all of the expected functions exist and accept the expected parameters
public class ApiTest {
  @Test
  public void testAlignOnce() {
    Api.alignOnce("ACGT", "ACGT", new AlignmentParameters(), Logger.NoOpLogger);
    Sequence querySequence = new SequenceBuilder().setName("query").add("ACGT").build();
    Api.alignOnce(new Query(querySequence), "ACGT", new AlignmentParameters(), Logger.NoOpLogger);
  }

  @Test
  public void testReusingDatabase() {
    String genome = "AACGTCGT";
    ReferenceDatabase referenceDatabase = Api.newDatabase(genome, Logger.NoOpLogger);
    Api.align("AACG", referenceDatabase, new AlignmentParameters(), Logger.NoOpLogger);
    Api.align("ACGT", referenceDatabase, new AlignmentParameters(), Logger.NoOpLogger);
  }

  @Test
  public void testCanUseCache() {
    StringWriter writer = new StringWriter();
    Logger logger = new Logger(writer);
    String genome = "AACCGT";
    ReferenceDatabase referenceDatabase = Api.newDatabase(genome, logger);
    Api.align("AACC", referenceDatabase, new AlignmentParameters(), logger);
    String marker = "reusing cached result";
    if (writer.getText().contains(marker)) {
      fail("First lookup reused cache result. Output: " + writer.getText());
    }

    // The cache isn't always enabled for all queries because we don't know how often it helps
    // So, we have to run this alignment a few times to ensure the cache should be enabled
    for (int i = 0; i < 3; i++) {
      Api.align("AACC", referenceDatabase, new AlignmentParameters(), logger);
    }
    boolean usedCache = writer.getText().contains(marker);
    if (!usedCache) {
      fail("Didn't use cache on subsequent lookup. Output: " + writer.getText());
    }
  }

  private void fail(String message) {
    Assert.fail(message);
  }
}
