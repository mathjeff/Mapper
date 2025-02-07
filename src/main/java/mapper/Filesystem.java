package mapper;

import java.io.File;
import java.io.IOException;

public interface Filesystem {
  boolean createNewFile(File file) throws IOException;
  void write(File file, byte[] content) throws IOException;
  byte[] readFile(File file) throws IOException;
  void mkdirs(File dir) throws IOException;
}
