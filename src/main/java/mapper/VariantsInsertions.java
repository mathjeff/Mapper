package mapper;

import java.util.ArrayList;
import java.util.List;

// A VariantsInsertion is essentially a Variants and a List<Variants>
public class VariantsInsertions extends Variants {
  public VariantsInsertions() {
  }

  public Variants getInsertions(int index) {
    if (this.insertions == null)
      return null;
    if (this.insertions.size() <= index)
      return null;
    return this.insertions.get(index);
  }

  public Variants getOrCreateInsertions(int index) {
    if (this.insertions == null)
      this.insertions = new ArrayList<Variants>();
    while (this.insertions.size() <= index) {
      this.insertions.add(new Variants());
    }
    return this.insertions.get(index);
  }

  private List<Variants> insertions;
}
