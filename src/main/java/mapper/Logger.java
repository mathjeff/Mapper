package mapper;

public class Logger {
  public static Logger NoOpLogger = new Logger(null, 1, 0);

  public Logger(TextWriter writer) {
    this.writer = writer;
    this.maxEnabledDepth = Integer.MAX_VALUE;
    this.initialize();
  }

  public Logger(TextWriter writer, int depth, int maxEnabledDepth) {
    this.writer = writer;
    this.depth = depth;
    this.maxEnabledDepth = maxEnabledDepth;
    this.initialize();
  }

  private void initialize() {
    this.prefix = this.computePrefix(this.depth);
    this.enabled = (this.depth <= this.maxEnabledDepth);
  }

  public void log(String message) {
    if (this.enabled) {
      this.writer.write(this.prefix + message.replace("\n", "\n" + this.prefix));
    } else {
      throw new IllegalArgumentException("Called log() on a disabled (depth = " + depth + ", maxEnabledDepth = " + maxEnabledDepth + ") with message '" + message + "'");
    }
  }

  public boolean getEnabled() {
    return this.enabled;
  }

  public void flush() {
    this.writer.flush();
  }

  public Logger incrementScope() {
    return new Logger(this.writer, this.depth + 1, this.maxEnabledDepth);
  }

  public Logger withWriter(TextWriter writer) {
    return new Logger(writer, this.depth, this.maxEnabledDepth);
  }

  private String computePrefix(int depth) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < depth; i++) {
      builder.append(" ");
    }
    return builder.toString();
  }

  private TextWriter writer;
  private int depth;
  private String prefix;
  private int maxEnabledDepth;
  private boolean enabled;
}
