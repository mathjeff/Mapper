package mapper;

import java.io.IOException;
import java.util.Properties;

// Returns metadata about Mapper
public class MapperMetadata {

  // The version of Mapper that is running
  public static String getVersion() {
    Properties properties = new Properties();
    try {
      properties.load(MapperMetadata.class.getResourceAsStream("/mapper.properties"));
    } catch (IOException e) {
      throw new RuntimeException("Failed to get Mapper version", e);
    }
    String version = properties.getProperty("mapper.version", "unknown");
    return version;
  }
}
