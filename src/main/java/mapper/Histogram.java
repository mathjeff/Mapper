package mapper;

import java.util.ArrayList;
import java.util.List;

public class Histogram {
  public static List<String> formatColumn(String title, String yName, String xName, double xMin, double xMax, int desiredNumBins, double[] counts) {
    double[] binCounts;
    if (desiredNumBins < counts.length)
      binCounts = squash(counts, desiredNumBins);
    else
      binCounts = counts;

    List<String> rows = new ArrayList<String>();
    int bodyIndentSize = 1; // size of indent of body compared to title
    String leftSpace = repeat(" ", yName.length() + bodyIndentSize); // empty space to the left of the y-axis

    rows.add(title);
    double maxCount = getMax(binCounts);
    int chartHeight = 10;
    int numColumns = binCounts.length;

    double[] scaledValues;
    if (maxCount > 0)
      scaledValues = rescale(binCounts, chartHeight / maxCount);
    else
      scaledValues = binCounts;

    // Plot the graph contents
    int middleRowIndex = chartHeight / 2 - 1;
    for (int y = chartHeight - 1; y >= 0; y--) {
      StringBuilder rowBuilder = new StringBuilder();
      if (y == middleRowIndex) {
        rowBuilder.append(repeat(" ", bodyIndentSize) + yName);
      } else {
        rowBuilder.append(leftSpace);
      }
      rowBuilder.append("|");
      for (int x = 0; x < numColumns; x++) {
        double difference = scaledValues[x] - y;
        // Use a different character at the top of the bar based on the content
        if (difference >= 0.8) {
          rowBuilder.append("#");
        } else {
          if (difference >= 0.6) {
            rowBuilder.append("^");
          } else {
            if (difference >= 0.4) {
              String marker = "-";
              if (x > 0 && x + 1 < numColumns) {
                if (scaledValues[x - 1] < scaledValues[x] && scaledValues[x] < scaledValues[x + 1])
                  marker = "/";
                if (scaledValues[x - 1] > scaledValues[x] && scaledValues[x] > scaledValues[x + 1])
                  marker = "\\";
              }
              rowBuilder.append(marker);
            } else {
              if (difference >= 0.2) {
                rowBuilder.append("_");
              } else {
                rowBuilder.append(" ");
              }
            }
          }
        }
        rowBuilder.append(" ");
      }
      rows.add(rowBuilder.toString());
    }
    rows.add(leftSpace + "." + repeat("--", numColumns));
    String lowerBoundFormatted = String.format("%.2f", xMin);
    String upperBoundFormatted = String.format("%.2f", xMax);
    String rangeContents = lowerBoundFormatted + repeat(" ", Math.max(1, numColumns * 2 - lowerBoundFormatted.length() - upperBoundFormatted.length())) + upperBoundFormatted;
    rows.add(leftSpace + " " + rangeContents);
    rows.add(leftSpace + " " + xName);
    return rows;
  }

  private static double getMax(double[] binCounts) {
    double maxCount = 0;
    for (int i = 0; i < binCounts.length; i++) {
      maxCount = Math.max(maxCount, binCounts[i]);
    }
    return maxCount;
  }

  private static double[] rescale(double[] binCounts, double multiplier) {
    double[] results = new double[binCounts.length];
    for (int i = 0; i < binCounts.length; i++) {
      results[i] = binCounts[i] * multiplier;
    }
    return results;
  }

  private static String repeat(String text, int count) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < count; i++) {
      builder.append(text);
    }
    return builder.toString();
  }

  public static double[] squash(double[] counts, int desiredNumBins) {
    double[] results = new double[desiredNumBins];
    double start = 0;
    for (int readIndex = 0; readIndex < counts.length; readIndex++) {
      double end = (double)(readIndex + 1) / (double)counts.length * (double)results.length;
      if ((int)start == (int)end) {
        results[(int)start] += counts[readIndex];
      } else {
        int lowIndex = (int)start;
        double lowerWeight = (lowIndex + 1 - start);
        double upperWeight = end - (int)(lowIndex + 1);
        double totalWeight = lowerWeight + upperWeight;
        int highIndex = lowIndex + 1;
        results[lowIndex] += counts[readIndex] * lowerWeight / totalWeight;
        if (highIndex < results.length)
          results[highIndex] += counts[readIndex] * upperWeight / totalWeight;
      }
      start = end;
    }
    return results;
  }
}
