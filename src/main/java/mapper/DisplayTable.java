package mapper;

import java.util.ArrayList;
import java.util.List;

public class DisplayTable {
  public DisplayTable() {
  }

  public void addColumn(List<String> column) {
    this.columns.add(column);
  }

  public void addShortColumn(String firstRow) {
    ArrayList<String> column = new ArrayList<String>();
    column.add(firstRow);
    this.columns.add(column);
  }

  public String format() {
    // get max length of each column
    List<Integer> columnLengths = new ArrayList<Integer>();
    for (int i = 0; i < this.columns.size(); i++) {
      int columnLength = 0;
      for (String item: this.columns.get(i)) {
        if (item.length() > columnLength)
          columnLength = item.length();
      }
      columnLengths.add(columnLength);
    }
    int numRows = 0;
    for (List<String> column: this.columns) {
      if (numRows < column.size())
        numRows = column.size();
    }

    StringBuilder result = new StringBuilder();
    for (int y = 0; y < numRows; y++) {
      for (int x = 0; x < this.columns.size(); x++) {
        String component = this.getComponent(x, y);
        while (component.length() < columnLengths.get(x)) {
          component += " ";
        }
        result.append(component);
      }
      result.append("\n");
    }
    return result.toString();
  }

  private String getComponent(int x, int y) {
    List<String> columns = this.columns.get(x);
    if (y >= columns.size())
      return "";
    return columns.get(y);
  }

  private List<List<String>> columns = new ArrayList<List<String>>();
}
