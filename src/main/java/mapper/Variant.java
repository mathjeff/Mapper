package mapper;

public class Variant {
  public Variant(char allele) {
    this.allele = allele;
  }

  public void setCount(int newCount) {
    this.count = newCount;
  }

  public int getCount() {
    return this.count;
  }

  public void addCount(int extraCount) {
    this.count += extraCount;
  }

  public void setExample(Sequence sequence, int index) {
    this.exampleSequence = sequence;
    this.exampleIndex = index;
  }

  public Sequence getExampleSequence() {
    return this.exampleSequence;
  }

  public int getExampleIndex() {
    return this.exampleIndex;
  }

  public char getAllele() {
    return this.allele;
  }

  private int count;
  private Sequence exampleSequence;
  private int exampleIndex;
  private char allele;
}
