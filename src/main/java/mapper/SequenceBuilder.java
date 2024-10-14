package mapper;

import java.util.ArrayList;

public class SequenceBuilder {
  public SequenceBuilder() {
    this.stringBuilder = new StringBuilder();
  }
  public SequenceBuilder setName(String name) {
    this.name = name;
    return this;
  }
  public void setPath(String path) {
    this.path = path;
  }

  private String compress(String text) {
    this.currentValue = 0;
    this.currentCount = 0;
    StringBuilder compressed = new StringBuilder();
    for (int i = 0; i < text.length(); i++) {
      char basePair = text.charAt(i);
      byte newValue = Basepairs.encode(basePair);

      this.currentValue += ((int)newValue << this.currentCount);
      this.currentCount += 4;
      if (this.currentCount >= 16) {
        compressed.append(this.emitChar());
      }
    }
    if (currentCount > 0) {
      compressed.append(this.emitChar());
    }
    return compressed.toString();
  }

  public SequenceBuilder add(char text) {
    stringBuilder.append(text);
    this.length++;
    return this;
  }

  public SequenceBuilder add(String text) {
    stringBuilder.append(text);
    this.length += text.length();
    return this;
  }

  public void setId(long identifier) {
    this.identifier = identifier;
  }

  public Sequence build() {
    String compressed = this.compress(this.stringBuilder.toString().toUpperCase());
    if (this.buildRead) {
      ReadSequence result = new ReadSequence(name, compressed, length, path);
      result.nameSuffix = this.nameSuffix;
      result.qualityString = this.qualityString;
      result.commentString = this.commentString;
      result.setId(this.identifier);
      return result;
    } else {
      Sequence result = new Sequence(name, compressed, length, path);
      result.setId(this.identifier);
      return result;
    }
  }

  public SequenceBuilder asRead(String nameSuffix, String qualityString, String commentString) {
    this.buildRead = true;
    this.nameSuffix = nameSuffix;
    this.qualityString = qualityString;
    this.commentString = commentString;
    return this;
  }

  public int getLength() {
    return this.length;
  }

  private char emitChar() {
    char newValue = (char)(this.currentValue % 65536);
    this.currentCount -= 16;
    this.currentValue = this.currentValue >> 16;
    return newValue;
  }

  private String name;
  private String path;
  StringBuilder stringBuilder;
  int currentValue;
  int currentCount;
  int length;
  boolean buildRead = false;
  String nameSuffix;
  String qualityString;
  String commentString;
  long identifier;
}
