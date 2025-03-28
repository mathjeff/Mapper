package mapper;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;


public class HashBlockDatabase_Test {
  public HashBlockDatabase_Test() {
  }

  @Test
  public void testConsistency() {
    Sequence a = new SequenceBuilder().setName("contig1").add("ACCCCCCC").build();
    Sequence b = new SequenceBuilder().setName("contig2").add("CTTTTTTT").build();
    List<Sequence> list1 = new ArrayList<Sequence>();
    list1.add(a);
    list1.add(b);
    SequenceDatabase s = new SequenceDatabase(list1, true);
    StatusLogger statusLogger = new StatusLogger(new Logger(new StderrWriter()), 0);
    HashBlock_Database db1 = new HashBlock_Database(s, 1, 1, -1, true, null, statusLogger, false);
    HashBlock_Database db2 = new HashBlock_Database(s, 1, 1, -1, true, null, statusLogger, true);
    db1.requireSetUpThroughSize(100);
    db2.requireSetUpThroughSize(100);
    db1.verifyMatches(db2);
  }

  private void fail(String message) {
    Assert.fail(message);
  }
}
