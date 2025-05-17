package mapper;

public class VcfFormatterWorker extends Thread {
  public VcfFormatterWorker(boolean includeNonMutations, boolean showSupportRead) {
    this.includeNonMutations = includeNonMutations;
    this.showSupportRead = showSupportRead;
  }

  public void request(VcfFormatRequest formatRequest) {
    this.formatRequest = formatRequest;
  }

  public String getResults() {
    return results;
  }

  public int getNumReferencePositionsMatched() {
    return this.numReferencePositionsMatched;
  }

  @Override
  public void run() {
    this.results = this.format(this.formatRequest);
  }

  private String format(VcfFormatRequest formatRequest) {
    this.outputBuilder = new StringBuilder();
    boolean showSupportRead = this.showSupportRead;
    String sequenceName = formatRequest.sequence.getName();
    FilteredAlignments bases = formatRequest.alignments;
    int endIndex = formatRequest.startIndex + formatRequest.length;
    for (int i = formatRequest.startIndex; i < endIndex; i++) {
      AlignmentPosition frequencies = bases.getPosition(i);
      int displayIndex = i + 1;
      if (frequencies.getCount() > 0) {
        if (this.includeNonMutations || frequencies.hasAlternates())
          writePosition(sequenceName, displayIndex, frequencies, showSupportRead);
        this.numReferencePositionsMatched++;
      }
      int insertionIndex = 0;
      while (true) {
        AlignmentPosition insertion = bases.getInsertion(i, insertionIndex);
        if (!insertion.hasAlternates())
          break;
        writePosition(sequenceName, -1 * displayIndex, insertion, showSupportRead);
        insertionIndex++;
      }
    }
    return this.outputBuilder.toString();
  }

  private void writePosition(String sequenceName, int rowNumber, AlignmentPosition frequencies, boolean showSupportRead) {
    appendText(sequenceName);
    newField();
    appendText(Integer.toString(rowNumber));
    newField();

    char reference = frequencies.getReference();
    appendText(reference);
    newField();
    char[] alternates = frequencies.getNonzeroAlternates(reference);
    appendJoined(',', alternates);
    newField();
    appendText(frequencies.formatCount());
    newField();

    writeDepths(frequencies, reference, alternates, false);
    newField();
    writeDepths(frequencies, reference, alternates, true);

    if (showSupportRead) {
      newField();
      boolean isFirst = true;
      for (char alternate : alternates) {
        // we found a mutation; let's show one sample read that mapped to this position
        Sequence sampleQuery = frequencies.getSampleAlternateSequence(alternate);
        if (isFirst) {
          isFirst = false;
        } else {
          appendText(",");
        }
        if (sampleQuery != null) {
          int index = frequencies.getSampleAlternateIndex(alternate);
          if (frequencies.isSampleAlternateDeletion(alternate)) {
            appendText(sampleQuery.getRange(0, index));
            appendText("[-]");
            appendText(sampleQuery.getRange(index, sampleQuery.getLength() - index));
          } else {
            appendText(sampleQuery.getRange(0, index));
            appendText('[');
            appendText(sampleQuery.charAt(index));
            appendText(']');
            appendText(sampleQuery.getRange(index + 1, sampleQuery.getLength() - 1 - index));
          }
        }
      }
    }
    newLine();
  }

  private void writeDepths(AlignmentPosition frequencies, char reference, char[] alternates, boolean isQueryEnd) {
    appendText(frequencies.getCounts(reference, isQueryEnd));
    for (char alternate : alternates) {
      appendText(';');
      appendText(frequencies.getCounts(alternate, isQueryEnd));
    }
  }

  private void appendJoined(char separator, char[] components) {
    boolean first = true;
    for (int i = 0; i < components.length; i++) {
      char component = components[i];
      if (first) {
        first = false;
      } else {
        this.appendText(separator);
      }
      this.appendText(component);
    }
  }

  private void appendText(String text) {
    this.outputBuilder.append(text);
    if (text != null && text.length() > 0)
      this.fieldNonempty = true;
  }

  private void appendText(char text) {
    this.outputBuilder.append(text);
    this.fieldNonempty = true;
  }

  private void endField() {
    if (!this.fieldNonempty) {
      // replace empty fields with a default value because it's more convenient for some users
      this.outputBuilder.append('.');
    }
  }

  private void newField() {
    this.endField();
    this.outputBuilder.append('\t');
    this.fieldNonempty = false;
  }

  private void newLine() {
    this.endField();
    this.outputBuilder.append('\n');
    this.fieldNonempty = false;
  }

  VcfFormatRequest formatRequest;
  StringBuilder outputBuilder;
  boolean fieldNonempty;
  String results;
  int numReferencePositionsMatched;
  boolean includeNonMutations;
  boolean showSupportRead;
}
