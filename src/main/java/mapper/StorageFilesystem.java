package mapper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

// A StorageFilesystem passes requests along to the actual filesystem
public class StorageFilesystem implements Filesystem {
  public static StorageFilesystem Instance = new StorageFilesystem();

  private StorageFilesystem() {
  }

  public boolean createNewFile(File file) throws IOException {
    file.getParentFile().mkdirs();
    return file.createNewFile();
  }

  public void write(File file, byte[] content) throws IOException {
    FileOutputStream fileStream = new FileOutputStream(file);
    fileStream.write(content);
    fileStream.close();
  }

  public byte[] readFile(File file) throws IOException {
    Path path = Paths.get(file.getAbsolutePath());
    return Files.readAllBytes(path);
  }

  public void mkdirs(File dir) throws IOException {
    dir.mkdirs();
  }
}
