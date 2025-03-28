package mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// A ByteKeyStore is a Map<Integer, Set<byte[]> that uses less memory
class ByteKeyStore {
  public ByteKeyStore(int numKeys, int maxCountPerKey, int numBitsPerValue) {
    this.cumulativeShortCounts = new short[numKeys];
    this.maxCountPerKey = maxCountPerKey;
    if (maxCountPerKey < 0)
      throw new IllegalArgumentException("maxCountPerKey = " + maxCountPerKey + " < 0");
    if (numBitsPerValue < 1)
      throw new IllegalArgumentException("numBitsPerValue = " + numBitsPerValue);
    this.numBitsPerValue = Math.max(8, numBitsPerValue); // need at least this many bits so we can determine the number of items from the number of bytes
    this.values = new byte[0];
  }

  // Tries to add the given bytes to the given store an returns whether it was added successfully
  // <key> must be from 0 to 255
  public void add(int key, byte[] bytes) {
    if (!this.knowsAllProcessedMatches(key))
      return; // we already have too many of this value to be interested in saving it
    //System.err.println("adding " + bytes.length + " bytes to key " + key);
    int expectedNumBytes = getNumBytesPerValue();
    if (bytes.length != expectedNumBytes) {
      throw new IllegalArgumentException("Expected " + this.numBitsPerValue + " bits (" + expectedNumBytes + " bytes), got " + bytes.length + " bytes");
    }

    if (this.pendingAdds == null) {
      this.pendingAdds = new ByteArrayList();
      int estimatedNumBytesRequired = this.getNumKeys() * this.getNumBytesPerValue();
      this.pendingAdds.ensureCapacity(estimatedNumBytesRequired);
    }

    // Now add more pending data
    this.pendingAdds.add((byte)key);
    //System.err.println("ByteKeyStore adding " + bytes.length + " bytes at key " + key);
    for (int i = 0; i < bytes.length; i++) {
      //System.err.println("ByteKeyStore adding byte " + bytes[i] + " at " + this.usedByteCount);
      this.pendingAdds.add(bytes[i]);
    }

    int estimatedNumBytesRequired = this.getNumKeys() * this.getNumBytesPerValue();
    if (this.pendingAdds.size() >= this.values.length && this.pendingAdds.size() >= estimatedNumBytesRequired) {
      // The pending adds are getting long so let's process them now
      this.processPendingAdds();
    }
  }

  public BytesView get(int key) {
    // process pending adds so the data will be where we expect it to be
    this.processPendingAdds();
    // allocate results
    if (!knowsAllProcessedMatches(key)) {
      //System.err.println("too many matches for key " + key);
      return null;
    }
    int numBytes = getNumBytesEncoded(key);
    int startIndex = getNumBytesBefore(key);
    BytesSlice bytes = new BytesSlice(this.values, startIndex, numBytes);
    BytesView decoded = this.decode(bytes);
    //System.err.println("in get(), numBytes[0] = " + getNumBytesEncoded(0));
    //System.err.println("get(" + key + ") encoded length = " + bytes.size() + " decoded length " + decoded.size());
    return decoded;
  }

  private int getIndex(int key, int offset) {
    return this.getNumBytesBefore(key) + offset;
  }

  private int getNumBytesBefore(int key) {
    if (key > 0)
      return getNumBytesThrough(key - 1);
    return 0;
  }

  // returns the number of items saved at keys before or equal to this one
  private int getNumBytesThrough(int key) {
    int value;
    if (cumulativeShortCounts != null)
      value = cumulativeShortCounts[key];
    else
      value = cumulativeIntCounts[key];
    if (value >= 0)
      return value;
    return -(value + 1);
  }

  private void putNumBytesThrough(int key, int count) {
    if (this.cumulativeShortCounts != null) {
      if (Math.abs(count) >= 32767) {
        // This count is too large, so convert to integer counts
        this.cumulativeIntCounts = new int[this.cumulativeShortCounts.length];
        for (int i = 0; i < this.cumulativeShortCounts.length; i++) {
          this.cumulativeIntCounts[i] = this.cumulativeShortCounts[i];
        }
        this.cumulativeShortCounts = null;
      } else {
        this.cumulativeShortCounts[key] = (short)count;
      }
    }
    if (this.cumulativeIntCounts != null) {
      this.cumulativeIntCounts[key] = count;
    }
  }

  public int getNumBytesEncoded(int key) {
    // process pending adds so the data will be where we expect it to be
    this.processPendingAdds();
    if (!knowsAllProcessedMatches(key))
      return Integer.MAX_VALUE;
    return this.getProcessedNumBytes(key);
  }

  public int getNumValues(int key) {
    // process pending adds so the data will be where we expect it to be
    this.processPendingAdds();
    if (!knowsAllProcessedMatches(key))
      return Integer.MAX_VALUE;

    int numBytes = getNumBytesEncoded(key);
    if (shouldCompress()) {
      int startIndex = getNumBytesBefore(key);
      BytesSlice bytes = new BytesSlice(this.values, startIndex, numBytes);
      return getNumEncodedItems(bytes);
    } else {
      return numBytes / getNumBytesPerValue();
    }
  }

  private int getProcessedNumBytes(int key) {
    if (!knowsAllProcessedMatches(key))
      return this.maxCountPerKey + 1;
    return getNumBytesThrough(key) - getNumBytesBefore(key);
  }

  public boolean knowsAllMatches(int key) {
    // process pending adds so the data will be where we expect it to be
    this.processPendingAdds();
    return this.knowsAllProcessedMatches(key);
  }

  // resolves any workin progress to reduce the memory usage as much as possible
  public void pack() {
    this.processPendingAdds();
  }

  public void writeTo(Serializer serializer) throws IOException {
    serializer.writeProperty("countPerKey", "" + this.maxCountPerKey);
    serializer.writeProperty("bitsPerValue", "" + this.numBitsPerValue);

    byte[] shortBytes = this.shortsToBytes(this.cumulativeShortCounts);
    serializer.writeBytesAndLength(shortBytes);

    byte[] intBytes = this.intsToBytes(this.cumulativeIntCounts);
    serializer.writeBytesAndLength(intBytes);

    serializer.writeBytesAndLength(this.values);
    serializer.writeString("\n");
  }

  public void readFrom(Deserializer deserializer) throws IOException {
    this.maxCountPerKey = deserializer.readIntProperty("countPerKey");
    this.numBitsPerValue = deserializer.readIntProperty("bitsPerValue");

    byte[] shortBytes = deserializer.readLengthPrefixedByteArray();
    this.cumulativeShortCounts = this.bytesToShorts(shortBytes);

    byte[] intBytes = deserializer.readLengthPrefixedByteArray();
    this.cumulativeIntCounts = this.bytesToInts(intBytes);

    this.values = deserializer.readLengthPrefixedByteArray();
    deserializer.readText("\n");

    this.pendingAdds = null;
  }

  private boolean knowsAllProcessedMatches(int key) {
    int count;
    if (cumulativeShortCounts != null)
      count = cumulativeShortCounts[key];
    else
      count = cumulativeIntCounts[key];
    return count >= 0;
  }

  private int getNumKeys() {
    if (this.cumulativeShortCounts != null)
      return this.cumulativeShortCounts.length;
    else
      return this.cumulativeIntCounts.length;
  }

  private int getNumBytesPerValue() {
    return (numBitsPerValue + 7) / 8;
  }

  private int getPendingAddStartIndex() {
    return this.getNumBytesThrough(this.getNumKeys() - 1);
  }

  private void processPendingAdds() {
    if (this.pendingAdds == null || this.pendingAdds.size() <= 0)
      return; // nothing to do
    //System.err.println("processing pending adds from " + pendingAddStartIndex);
    // allocate new counts
    int numKeys = this.getNumKeys();
    int[] newCounts = new int[numKeys];
    for (int i = 0; i < numKeys; i++) {
      newCounts[i] = this.getProcessedNumBytes(i);
    }
    // update counts based on pending adds
    int numBytesPerValue = (this.numBitsPerValue + 7) / 8;
    if (numBytesPerValue < 1)
      throw new IllegalArgumentException("numBytesPerValue = " + numBytesPerValue);
    int numBytesPerPendingAdd = 1 + numBytesPerValue;
    for (int i = 0; i < this.pendingAdds.size(); i += numBytesPerPendingAdd) {
      int newKey = this.pendingAdds.get(i);
      if (newKey < 0)
        newKey += 256;
      //System.err.println("adding " + numBytesPerValue + " bytes capacity for key " + newKey);
      newCounts[newKey] += numBytesPerValue;
    }
    // clear any counts that are already too large
    for (int i = 0; i < newCounts.length; i++) {
      if (newCounts[i] > this.maxCountPerKey) {
        //System.err.println("clearing excessive capacity of " + newCounts[i] + " for key " + i);
        newCounts[i] = -1;
      }
    }

    // copy the existing values into a new array
    ArrayList<ByteArrayList> newValues = new ArrayList<ByteArrayList>();
    for (int i = 0; i < numKeys; i++) {
      newValues.add(null);
    }
    for (int key = numKeys - 1; key >= 0; key--) {
      int decodedNumBytes = newCounts[key];
      if (decodedNumBytes < 0)
        continue; // already had too many values to store for this key

      int startIndex;
      if (key > 0) {
        startIndex = newCounts[key - 1];
        if (startIndex < 0)
          startIndex = -(startIndex+1);
      } else {
        startIndex = 0;
      }

      ByteArrayList encodedValuesHere = new ByteArrayList();
      int numBytesHere = this.getProcessedNumBytes(key);
      encodedValuesHere.ensureCapacity(numBytesHere);

      for (int offset = 0; offset < numBytesHere; offset++) {
        int oldIndex = this.getIndex(key, offset);
        //System.err.println("Copying key " + key + " offset " + offset + " old index " + oldIndex + " value " + this.values[oldIndex]);
        encodedValuesHere.put(offset, this.values[oldIndex]);
      }
      ByteArrayList valuesHere = this.decode(encodedValuesHere).writable();
      newValues.set(key, valuesHere);

      // update counts to refer to the next index to write to
      newCounts[key] = valuesHere.size();
    }

    // copy the pending adds into the new store
    int firstNewCount = newCounts[0];
    for (int i = 0; i < this.pendingAdds.size(); i += numBytesPerPendingAdd) {
      int key = this.pendingAdds.get(i);
      if (key < 0)
        key += 256;
      if (newCounts[key] < 0)
        continue; // already had too many values to store for this key
      int startIndex = newCounts[key];
      ByteArrayList bin = newValues.get(key);
      for (int offset = 0; offset < numBytesPerValue; offset++) {
        int index = startIndex + offset;
        //System.err.println("applying pending add: key = " + key + " offset = " + offset + " dest index = " + index + " numBytesPerPending add " + numBytesPerPendingAdd + " numBytesPerValue " + numBytesPerValue + " value " + this.pendingAdds.get(i + 1 + offset));
        bin.put(index, this.pendingAdds.get(i + 1 + offset));
      }
      newCounts[key] = startIndex + numBytesPerValue;
      if (bin.size() > this.maxCountPerKey) {
        newCounts[key] = -1;
        newValues.set(key, null);
      }
    }

    // update
    this.put(newValues);
    //outputDiagnostics();
  }

  // updates our state to match the given decoded values
  private void put(ArrayList<ByteArrayList> decodedValues) {
    // encode
    List<ByteArrayList> encodedValues = new ArrayList<ByteArrayList>();
    for (int i = 0; i < decodedValues.size(); i++) {
      encodedValues.add(this.encode(decodedValues.get(i)));
    }
    // compute total encoding length
    int totalNumEncodedBytes = 0;
    for (int i = 0; i < encodedValues.size(); i++) {
      ByteArrayList encodedValuesHere = encodedValues.get(i);
      if (encodedValuesHere == null) {
        continue;
      }
      int encodedNumBytes = encodedValuesHere.size();
      totalNumEncodedBytes += encodedNumBytes;
    }
    // allocate data store
    int desiredLength = totalNumEncodedBytes;
    //System.err.println("Expanding to new byte length " + desiredLength);
    byte[] encoded = new byte[desiredLength];
    int writeIndex = 0;
    // write values
    for (int i = 0; i < encodedValues.size(); i++) {
      ByteArrayList encodedValuesHere = encodedValues.get(i);
      if (encodedValuesHere == null) {
        this.putNumBytesThrough(i, -(writeIndex+1));
        continue;
      }
      for (int j = 0; j < encodedValuesHere.size(); j++) {
        encoded[writeIndex] = encodedValuesHere.get(j);
        writeIndex++;
      }
      this.putNumBytesThrough(i, writeIndex);
      //System.err.println("putNumBytesThrough(" + i + ", " + writeIndex + ")");
    }
    // save
    this.values = encoded;
    //System.err.println("put() completed. usedByteCount = " + usedByteCount);
    this.pendingAdds = null;
  }

  private void outputDiagnostics() {
    //System.err.println("key store with " + this.getNumKeys() + " keys and " + this.numBitsPerValue + " bits per value:");
    for (int key = 0; key < this.getNumKeys(); key++) {
      int count = this.getProcessedNumBytes(key);
      if (count != 0) {
        if (knowsAllProcessedMatches(key)) {
          //System.err.println("Byte " + key + " count " + count);
          for (int offset = 0; offset < count; offset++) {
            int index = getIndex(key, offset);
            byte value = this.values[index];
            //System.err.println("values[" + index + "] = " + value);
          }
        } else {
          //System.err.println("Byte " + key + " count " + count + " (not stored)");
        }
      }
    }
    //System.err.println("");
  }

  private ByteArrayList encode(ByteArrayList data) {
    if (data == null || data.size() < 1)
      return data;
    // sort the data so the consecutive differences are small and positive
    sort(data);
    if (!shouldCompress())
      return data;

    // compute some sizes
    int numBytesPerDecodedValue = this.getNumBytesPerValue();
    int numItems = data.size() / numBytesPerDecodedValue;
    int numStepBits = chooseNumStepBits(numItems);
    long stepSize = (long)1 << (long)numStepBits;
    
    // write values in order
    long previousValue = 0;
    ByteArrayList result = new ByteArrayList();
    int numBitsWritten = 0;
    for (int readIndex = 0; readIndex < data.size(); readIndex += numBytesPerDecodedValue) {
      long currentValue = this.bytesToLong(data, readIndex * 8, this.numBitsPerValue);
      //System.err.println("read currentValue " + currentValue + " at bit " + readIndex);
      // Encode any large jumps
      while (currentValue >= previousValue + stepSize) {
        this.appendData(0, numBitsWritten, 1, result);
        previousValue += stepSize;
        numBitsWritten++;
      }
      // Write that there are no more large jumps remaining
      this.appendData(1, numBitsWritten, 1, result);
      numBitsWritten++;
      long difference = currentValue - previousValue;
      // Write the difference
      //System.err.println("Writing " + difference + "(" + numStepBits + " bits) at " + numBitsWritten);
      this.appendData(difference, numBitsWritten, numStepBits, result);
      numBitsWritten += numStepBits;
      previousValue = currentValue;
    }
    // Also write the number of items at the end
    int numCountBits = chooseNumCountBits(numBitsWritten);
    int totalNumBits = numBitsWritten + numCountBits;
    int totalNumBytes = (totalNumBits + 7) / 8;
    int countStartIndex = totalNumBytes * 8 - numCountBits;
    while (countStartIndex < numBitsWritten) {
      countStartIndex += 8;
    }
    //System.err.println("writing count " + numItems + " at bit index " + countStartIndex + " using " + numCountBits + " bits");
    this.appendData(numItems, countStartIndex, numCountBits, result);
    //System.err.println("encoded data to length " + result.size());
    return result;
  }

  private boolean shouldCompress() {
    // Bins that have a small maxCountPerKey are for shorter hashblocks
    // Results for shorter hashblocks are more likely to be used, so it costs more total time to decompress them
    // Results for longer hashblocks are less likely to be as commonly used, so we compress them
    return this.maxCountPerKey >= 800;
  }

  // Given the number of items in the set, determines how many bits to use to encode a position
  private int chooseNumStepBits(int numItems) {
    long maxPossibleValue = ((long)1 << (long)this.numBitsPerValue) - (long)1;
    long typicalStep = (maxPossibleValue + (long)numItems - (long)1) / (long)numItems;
    return log2RoundUp(typicalStep / 2);
  }

  private BytesView decode(BytesView data) {
    if (data == null || data.size() < 1 || !shouldCompress())
      return data;
    //System.err.println("");
    //System.err.println("decoding data of length " + data.size());
    // compute some sizes
    int numItems = getNumEncodedItems(data);
    if (numItems < 1) {
      throw new IllegalArgumentException("getNumEncodedItems for data of length " + data.size() + " returned " + numItems);
    }
    int numStepBits = chooseNumStepBits(numItems);
    long stepSize = (long)1 << (long)numStepBits;
    int numBytesPerDecodedValue = this.getNumBytesPerValue();
    int numBitsPerDecodedValue = numBytesPerDecodedValue * 8;

    // read values in order
    long currentValue = 0;
    int numBitsRead = 0;
    ByteArrayList result = new ByteArrayList();
    int numItemsRead = 0;
    while (numItemsRead < numItems) {
      long currentBit = bytesToLong(data, numBitsRead, 1);
      numBitsRead++;
      if (currentBit == 0) {
        //System.err.println("Skipping bit " + numBitsRead + " with value 0");
        // There isn't a value in this section so we jump by a large amount and repeat
        currentValue += stepSize;
        continue;
      }
      // There is value in this range so we read it and repeat
      long readValue = bytesToLong(data, numBitsRead, numStepBits);
      //System.err.println("Read " + readValue + "(" + numStepBits + " bits) at " + numBitsRead);
      currentValue += readValue;
      numBitsRead += numStepBits;
      appendData(currentValue, numItemsRead * numBitsPerDecodedValue, numBitsPerValue, result);
      numItemsRead++;
    }
    //System.err.println("decoded data to length " + result.size());
    for (int i = 0; i < result.size(); i++) {
      //System.err.println("result[" + i + "] = " + result.get(i));
    }
    return result;
  }

  private int getNumEncodedItems(BytesView data) {
    int numCountBits = getNumCountBits(data.size() * 8);
    int numBits = data.size() * 8;
    int countStartBit = numBits - numCountBits;
    int count = (int)bytesToLong(data, countStartBit, numCountBits);
    //System.err.println("getNumEncodedItems from data of length " + data.size() + " using bits[" + countStartBit + ":" + (countStartBit + numCountBits) + "] = " + count);
    return count;
  }

  private int getNumCountBits(int totalNumBits) {
    int minNumBitsPerEncodedValue = Math.max(1, this.numBitsPerValue - this.log2RoundUp(this.maxCountPerKey));
    int maxNumPositions = totalNumBits / minNumBitsPerEncodedValue;
    int result = log2RoundUp(maxNumPositions + 1);
    //System.err.println("getNumCountBits(" + totalNumBits + ") = " + result);
    return result;
  }

  private int chooseNumCountBits(int numDataBits) {
    int numCountBits = 0;
    while (true) {
      int numBytes = (numCountBits + numDataBits + 7) / 8;
      int expectedNumCountBits = getNumCountBits(numBytes * 8);
      if (expectedNumCountBits > numCountBits) {
        numCountBits = expectedNumCountBits;
      } else {
        //System.err.println("chooseNumCountBits(" + numDataBits + ") = " + numCountBits);
        return numCountBits;
      }
    }
  }

  private long bytesToLong(BytesView data, int bitStart, int numBits) {
    long result = 0;
    int firstByteIndex = bitStart / 8;
    int lastByteIndex = (bitStart + numBits + 7) / 8;
    long shift = 0;
    for (int i = firstByteIndex; i < lastByteIndex; i++) {
      long byteHere = data.get(i);
      if (byteHere < 0)
        byteHere += 256;
      result += (byteHere << shift);
      shift += 8;
    }
    long numSkippedBits = bitStart - (firstByteIndex * 8);
    result = result >> numSkippedBits;
    result = result & ((1L << (long)numBits) - 1L);
    return result;
  }

  private void appendData(long value, int numBitsWritten, int numNewBits, ByteArrayList result) {
    int numBytesWritten = (numBitsWritten + 7) / 8;
    int numBitsRemainingInLastByte = numBytesWritten * 8 - numBitsWritten;

    long pendingValue = value;
    int numBytesToWrite;
    int writeIndex;

    if (numBitsRemainingInLastByte > 0) {
      if (numBytesWritten <= result.size()) {
        int lastByte = result.get(result.size() - 1);
        if (lastByte < 0)
          lastByte += 256;
        int numBitsUsedInLastByte = 8 - numBitsRemainingInLastByte;
        pendingValue = lastByte + (pendingValue << numBitsUsedInLastByte);
        numBytesToWrite = (numNewBits + numBitsUsedInLastByte + 7) / 8;
        writeIndex = result.size() - 1;
      } else {
        int numBitsUsedInLastByte = 8 - numBitsRemainingInLastByte;
        pendingValue = (pendingValue << numBitsUsedInLastByte);
        numBytesToWrite = (numNewBits + numBitsUsedInLastByte + 7) / 8;
        writeIndex = result.size();
      }
    } else {
      pendingValue = value;
      numBytesToWrite = (numNewBits + 7) / 8;
      writeIndex = numBytesWritten;
    }

    for (int i = 0; i < numBytesToWrite; i++) {
      byte b = (byte)(pendingValue & 255);
      //System.err.println("  appendData writing " + b + " at " + writeIndex + " work value = " + pendingValue);
      result.put(writeIndex, b);
      pendingValue = pendingValue >> 8;
      writeIndex++;
    }
  }

  private int log2RoundUp(long value) {
    if (value <= 1)
      return 0;
    int numBits = 1;
    long exponent = 2;
    while (true) {
      if (exponent >= value) {
        return numBits;
      }
      numBits++;
      exponent *= 2;
    }
  }

  private void sort(ByteArrayList data) {
    int numBytesPerValue = getNumBytesPerValue();
    int numValues = data.size() / numBytesPerValue;
    int delta = numValues / 2;
    while (delta > 0) {
      int numChanges = 0;
      for (int i = 0; i < numValues - delta; i++) {
        int lowIndex = i * numBytesPerValue;
        int highIndex = (i + delta) * numBytesPerValue;
        int comparison = compare(data, lowIndex, highIndex, numBytesPerValue);
        if (comparison < 0) {
          for (int j = 0; j < numBytesPerValue; j++) {
            byte a = data.get(lowIndex + j);
            byte b = data.get(highIndex + j);
            data.put(lowIndex + j, b);
            data.put(highIndex + j, a);
          }
          numChanges++;
        }
      }
      if (delta > 1) {
        if (numChanges <= (delta + 2) / 3)
          delta /= 2;
      } else {
        if (numChanges == 0)
          delta = 0;
      }
    }
  }

  private int compare(ByteArrayList data, int index1, int index2, int numBytes) {
    for (int i = numBytes - 1; i >= 0; i--) {
      int a = data.get(index1 + i);
      if (a < 0)
        a += 256;
      int b = data.get(index2 + i);
      if (b < 0)
        b += 256;
      if (a != b) {
        if (b > a)
          return 1;
        else
          return -1;
      }
    }
    return 0;
  }

  private byte[] shortsToBytes(short[] shorts) {
    if (shorts == null)
      return null;
    byte[] result = new byte[shorts.length * 2];
    int writeIndex = 0;
    for (int i = 0; i < shorts.length; i++) {
      short valueHere = shorts[i];
      result[writeIndex] = (byte)(valueHere & 255);
      writeIndex++;
      valueHere = (short)(valueHere >> 8);
      result[writeIndex] = (byte)(valueHere);
      writeIndex++;
    }
    return result;
  }
  private short[] bytesToShorts(byte[] bytes) {
    if (bytes.length == 0)
      return null;
    short[] result = new short[bytes.length / 2];
    for (int i = 0; i < result.length; i++) {
      short value = 0;
      value += (short)byteToInt(bytes[i * 2]);
      value += (short)byteToInt(bytes[i * 2 + 1]) << 8;
      result[i] = value;
    }
    return result;
  }

  private byte[] intsToBytes(int[] ints) {
    if (ints == null)
      return null;
    byte[] result = new byte[ints.length * 4];
    int writeIndex = 0;
    for (int i = 0; i < ints.length; i++) {
      int valueHere = ints[i];
      result[writeIndex + 3] = (byte)(valueHere & 255);
      valueHere = valueHere >> 8;
      result[writeIndex + 2] = (byte)(valueHere & 255);
      valueHere = valueHere >> 8;
      result[writeIndex + 1] = (byte)(valueHere & 255);
      valueHere = valueHere >> 8;
      result[writeIndex] = (byte)(valueHere & 255);
      writeIndex += 4;
    }
    return result;
  }

  private int[] bytesToInts(byte[] bytes) {
    if (bytes.length == 0)
      return null;

    int[] result = new int[bytes.length / 4];
    int readIndex = 0;
    for (int i = 0; i < result.length; i++) {
      int value = 0;
      value += byteToInt(bytes[readIndex]);
      value = value << 8;
      readIndex++;
      value += byteToInt(bytes[readIndex]);
      value = value << 8;
      readIndex++;
      value += byteToInt(bytes[readIndex]);
      value = value << 8;
      readIndex++;
      value += byteToInt(bytes[readIndex]);
      readIndex++;

      result[i] = value;
    }
    return result;
  }

  private int byteToInt(byte b) {
    if (b < 0)
      return (int)b + 256;
    return b;
  }

  int maxCountPerKey;
  int numBitsPerValue;
  short[] cumulativeShortCounts;
  int[] cumulativeIntCounts;
  byte[] values;
  ByteArrayList pendingAdds;
}
