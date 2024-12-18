package mapper;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Assert;
import org.junit.Test;

public class XMapperMetadata_Test {
  @Test
  public void simplificationOfSubdir() {
    Path a = Paths.get("/a");
    Path abc = Paths.get("/a/b/c");
    Path abcFromA = XMapperMetadata.simplifyPath(abc, a);
    // "b/c" should be simpler than "/a/b/c"
    checkPathsEqual(abcFromA, Paths.get("b/c"));
  }

  @Test
  public void simplificationOfNearParentDir() {
    Path ab = Paths.get("/a/b");
    Path abc = Paths.get("/a/b/c");
    Path abFromAbc = XMapperMetadata.simplifyPath(ab, abc);
    // ".." should be simpler than "/a/b"
    checkPathsEqual(abFromAbc, Paths.get(".."));
  }

  @Test
  public void simplificationOfFarParentDir() {
    Path a = Paths.get("/a");
    Path abc = Paths.get("/a/b/c");
    Path aFromAbc = XMapperMetadata.simplifyPath(a, abc);
    // "/a" should be simpler than "../.."
    checkPathsEqual(aFromAbc, a);
  }

  private void checkPathsEqual(Path a, Path b) {
    Assert.assertEquals(a, b); // we want a comparison that works across platforms
  }

}
