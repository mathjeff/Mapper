package mapper;

// A Variants is essentially a Map<Char, Variant>
public class Variants {
  public Variants() {
  }

  public Variant get(char allele) {
    if (this.variants != null) {
      for (Variant variant: this.variants) {
        if (variant.getAllele() == allele)
          return variant;
      }
    }
    return null;
  }

  public Variant getOrCreate(char allele) {
    Variant variant = this.get(allele);
    if (variant != null)
      return variant;
    int numVariants = this.getNumVariants();
    Variant[] newVariants = new Variant[numVariants + 1];
    for (int i = 0; i < numVariants; i++) {
      newVariants[i] = this.variants[i];
    }
    Variant result = new Variant(allele);
    newVariants[numVariants] = result;
    this.variants = newVariants;
    return result;
  }

  public Variant[] getAll() {
    return this.variants;
  }

  private int getNumVariants() {
    if (this.variants == null)
      return 0;
    return this.variants.length;
  }

  private Variant[] variants;
}
