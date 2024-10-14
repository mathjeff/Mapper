package mapper;

import java.util.HashMap;
import java.util.Map;

// An OverriddenSequence is a sequence that is based on another Sequence plus some changes
public class OverriddenSequence extends Sequence {
  public OverriddenSequence(Sequence original, String name) {
    super(name, null, original.getLength(), original.getPath());
    this.original = original;
  }

  /*public void put(Integer offset, char value) {
    byte encoded = Basepairs.encode(value);
    this.putEncoded(offset, encoded);
  }*/

  public void putEncoded(Integer offset, Byte value) {
    Byte existingOverride = this.overrides.get(offset);
    if (existingOverride != null) {
      throw new IllegalArgumentException("Cannot override " + this.getName() + "[" + offset + "] to " + Basepairs.decode(value) + " because it is already overridden to " + Basepairs.decode(existingOverride));
    }
    //System.out.println("overriding " + this.getName() + "[" + offset + "] = " + Basepairs.decode(value));

    this.overrides.put(offset, value);
  }

  @Override
  protected byte computeEncodedCharAt(int index) {
    Byte overridden = this.overrides.get(index);
    if (overridden != null)
      return overridden;
    return this.original.encodedCharAt(index);
  }

  private Sequence original;
  private Map<Integer, Byte> overrides = new HashMap<Integer, Byte>();
}
