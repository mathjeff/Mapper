package mapper;

import java.util.ArrayList;
import java.util.List;

// A HashBlock_Pyramid is essentially a List<List<HashBlock>>.
// See HashBlock_Stream for details
public class HashBlock_Pyramid {
  public HashBlock_Pyramid(HashBlock_Stream stream) {
    this.stream = stream;
    this.blocks = new ArrayList<HashBlock_Row>();
  }


  public HashBlock_Row get(int index) {
    while(true) {
      if (this.blocks.size() > index) {
        return this.blocks.get(index);
      }
      if (!this.addBatch()) {
        return null;
      }
    }
  }

  private boolean addBatch() {
    HashBlock_Row next = this.stream.getNextBatch();
    if (next == null)
      return false;
    this.blocks.add(next);
    return true;
  }

  private HashBlock_Stream stream;
  private List<HashBlock_Row> blocks;
}
