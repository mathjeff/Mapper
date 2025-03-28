package mapper;

import java.util.List;

public interface TextWriter {
  
  void write(String message);

  void write(List<String> messages);

  void flush();
}
