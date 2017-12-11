package cc.twittertools.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;

public class QueryLikelihoodModel {

  private final float mu;

  public QueryLikelihoodModel(float mu) {
    this.mu = mu;
  }

  public QueryLikelihoodModel() {
    this(2500);
  }

  public Map<String, Float> parseQuery(Analyzer analyzer, String query) throws IOException {
    String[] phrases = query.trim().split("[,\\s]+");
    Map<String, Float> weights = new HashMap<String, Float>();
    for(String phrase: phrases) {
      if (phrase.length() == 0) {
        continue;
      }
      
      String stem = null;
      float weight = 0.0f;
      if (phrase.contains("^")) {
        String term = phrase.split("\\^")[0];
        stem = AnalyzerUtils.stem(analyzer, term);
        weight = Float.parseFloat(phrase.split("\\^")[1]);

      } else {
        stem = AnalyzerUtils.stem(analyzer, phrase);
        weight = 1.0f/phrases.length;
      }
      if (stem.length() == 0) {
        continue;
      }

      if (weights.containsKey(stem)) {
        weight = weights.get(stem) + weight;
      }
      weights.put(stem, weight);
    }

    return weights;
  }
  
  public double computeQLScore(Map<String, Float> weights, Map<String, Long> ctfs, Map<String, Integer> docVector, long sumTotalTermFreq) throws IOException {
    int docLen = 0;
    for (Integer i : docVector.values()) {
      docLen += Math.abs(i);
    }

    double score = 0;
    for(String queryTerm: weights.keySet()) {
      float weight = weights.get(queryTerm);
      long ctf = ctfs.get(queryTerm);
      if (ctf == 0) continue;
      int tf = docVector.containsKey(queryTerm) ? docVector.get(queryTerm) : 0;
      score += weight * Math.log((tf + mu*((double)ctf/sumTotalTermFreq)) / (docLen + mu));
      //System.out.println("term: " + queryTerm + " freq in doc: " + tf
      //    + " freq in corpus: " + ctf);
    }
    return score;
  }
}
