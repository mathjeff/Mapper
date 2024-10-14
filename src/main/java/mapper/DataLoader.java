package mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

// A DataLoader reads files (such as .fasta files) and returns the corresponding representation of them
public class DataLoader {
  public static SequenceProvider LoadFrom(List<String> paths, boolean keepQualityData) throws IllegalArgumentException, IOException, FileNotFoundException {
    List<SequenceProvider> providers = new ArrayList<SequenceProvider>();
    for (String path : paths) {
      providers.add(readSequencesFrom(path, keepQualityData));
    }
    return new SequencesIterator(providers);
  }

  public static SequenceProvider LoadFrom(String path, boolean keepQualityData) throws IllegalArgumentException, IOException, FileNotFoundException {
    return readSequencesFrom(path, keepQualityData);
  }

  private static SequenceProvider readSequencesFrom(String path, boolean keepQualityData) throws IllegalArgumentException, IOException, FileNotFoundException {
    String effectivePath = path;

    InputStream inputStream = new FileInputStream(path);
    String gzipSuffix = ".gz";
    if (path.endsWith(gzipSuffix)) {
      inputStream = new GZIPInputStream(inputStream);
      effectivePath = effectivePath.substring(0, effectivePath.length() - gzipSuffix.length());
    }
    
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

    if (effectivePath.endsWith(".fasta") || effectivePath.endsWith(".fa") || effectivePath.endsWith(".fna")) {
      return new FastaParser(reader, path);
    }
    if (effectivePath.endsWith(".fastq") || effectivePath.endsWith(".fq") || effectivePath.endsWith(".ca")) {
      return new FastqParser(reader, path, keepQualityData);
    }

    throw new IllegalArgumentException("Unrecognized file extension: " + effectivePath + ", not .fasta/.fa or .fastq/.fq/.ca");
  }
}
