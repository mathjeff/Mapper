package mapper;

public interface BytesView {
  int size();
  byte get(int index);
  ByteArrayList writable();
}
