package mapper;

class OrderingUtils {
  static SequencePosition[] orderDeterministically(SequencePosition[] items) {
    // handle base cases
    if (items == null)
      return null;
    if (items.length < 2)
      return items;
    // allocate results array
    SequencePosition[] results = new SequencePosition[items.length];

    // hash items into bins and count collisions
    int numCollisions = 0;
    for (int i = 0; i < items.length; i++) {
      SequencePosition item = items[i];
      int index = chooseIndex(items[i], items.length);
      SequencePosition existing = results[index];
      if (existing == null) {
        results[index] = item;
      } else {
        if (item.compareTo(existing) > 0) {
          results[index] = item;
        }
        numCollisions++;
      }
    }

    if (numCollisions < 1)
      return results;

    // identify items that we didn't have space for
    int collisionIndex = 0;
    SequencePosition[] collisions = new SequencePosition[numCollisions];
    for (int i = 0; i < items.length; i++) {
      SequencePosition item = items[i];
      int index = chooseIndex(items[i], items.length);
      if (results[index] != item) {
        collisions[collisionIndex] = item;
        collisionIndex++;
      }
    }
    SequencePosition[] deterministicCollisions = orderDeterministically(collisions);

    // put leftovers back into the results array
    int writeIndex = 0;
    for (int i = 0; i < deterministicCollisions.length; i++) {
      while (results[writeIndex] != null) {
        writeIndex++;
      }
      results[writeIndex] = deterministicCollisions[i];
    }

    return results;
  }

  static int chooseIndex(SequencePosition item, int numItems) {
    return item.getStartIndex() % numItems;
  }

}
