package mapper;

import java.io.File;
import org.junit.Assert;
import org.junit.Test;

public class MemoryFilesystem_Test {
  @Test
  public void simpleTest() {
    MemoryFilesystem filesystem = new MemoryFilesystem();
    File f1 = new File("a");
    File f2 = new File("b");

    // files missing
    Assert.assertEquals(filesystem.readFile(f1), null);
    Assert.assertEquals(filesystem.readFile(f2), null);

    // make new file
    Assert.assertEquals(filesystem.createNewFile(f1), true);

    // one empty file should exist
    Assert.assertEquals(filesystem.readFile(f1).length, 0);
    Assert.assertEquals(filesystem.readFile(f2), null);

    // write text
    byte[] contents = "sample text".getBytes();
    filesystem.write(f1, contents);

    // one file should have the same contents
    Assert.assertEquals(filesystem.readFile(f1), contents);
    Assert.assertEquals(filesystem.readFile(f2), null);
  }
}
