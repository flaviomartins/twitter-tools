package cc.twittertools.util;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

public class AnalyzerUtils {

  public static List<String> analyze(Analyzer analyzer, String text) {
    List<String> result = new LinkedList<String>();
    try {
      TokenStream stream;
      stream = analyzer.tokenStream(null, new StringReader(text));

      CharTermAttribute charTermAttribute = stream.addAttribute(CharTermAttribute.class);
      stream.reset();
      while (stream.incrementToken()) {
        String term = charTermAttribute.toString();
        result.add(term);
      }
      stream.close();
    } catch (IOException e) {
      // do nothing
    }
    return result;
  }

  public static String stem(Analyzer analyzer, String term) throws IOException {
    TokenStream stream = analyzer.tokenStream(null, new StringReader(term));

    CharTermAttribute charTermAttribute = stream.addAttribute(CharTermAttribute.class);
    stream.reset();
    stream.incrementToken();
    String stem = charTermAttribute.toString();
    stream.close();
    return stem;
  }

}
