package mapper;

// a FourInt is just four ints. It can be used as a key in a HashMap
public class FourInt {
  public FourInt(int a, int b, int c, int d) {
    this.a = a;
    this.b = b;
    this.c = c;
    this.d = d;
  }

  @Override
  public boolean equals(Object otherObject) {
    FourInt other = (FourInt)otherObject;
    if (this.a != other.a)
      return false;
    if (this.b != other.b)
      return false;
    if (this.c != other.c)
      return false;
    if (this.d != other.d)
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    return a + b * 11 + c * 101 + d * 1063;
  }

  private int a, b, c, d;
}
