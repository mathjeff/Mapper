package mapper;

import org.junit.Test;

// The ApiTest class checks that all of the expected functions exist and accept the expected parameters
public class ApiTest {
  @Test
  public void testAlignOnce() {
    Api.alignOnce("ACGT", "ACGT", new AlignmentParameters(), Logger.NoOpLogger);
    Sequence querySequence = new SequenceBuilder().setName("query").add("ACGT").build();
    Api.alignOnce(new Query(querySequence), "ACGT", new AlignmentParameters(), Logger.NoOpLogger);
  }
}
