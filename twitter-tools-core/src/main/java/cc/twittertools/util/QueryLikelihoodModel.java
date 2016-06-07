package cc.twittertools.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import cc.twittertools.index.IndexStatuses;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

public class QueryLikelihoodModel {
  private static final String FIELD = IndexStatuses.StatusField.TEXT.name;
  private static final Analyzer ANALYZER = IndexStatuses.ANALYZER;

  private IndexReader reader;
  private final float mu;

  public QueryLikelihoodModel(IndexReader reader, float mu) {
    this.reader = reader;
    this.mu = mu;
  }

  public QueryLikelihoodModel(IndexReader reader) {
    this(reader, 2500);
  }

  //tokenize a term using TweetAnalyzer(stem=true)
  private String stemTerm(String term) throws IOException {
    TokenStream stream = null;
    stream = ANALYZER.tokenStream(FIELD, new StringReader(term));

    CharTermAttribute charTermAttribute = stream.addAttribute(CharTermAttribute.class);
    stream.reset();
    stream.incrementToken();
    String stemTerm = charTermAttribute.toString();
    stream.close();
    return stemTerm;
  }

  public Map<String, Float> parseQuery(String query) throws IOException {
    String[] phrases = query.trim().split("[,\\s]+");
    Map<String, Float> weights = new HashMap<String, Float>();
    for(String phrase: phrases) {
      if (phrase.length() == 0) {
        continue;
      }
      
      String tokenizeTerm = null;
      float weight = 0.0f;
      if (phrase.contains("^")) {
        String term = phrase.split("\\^")[0];
        tokenizeTerm = stemTerm(term);
        weight = Float.parseFloat(phrase.split("\\^")[1]);

      } else {
        tokenizeTerm = stemTerm(phrase);
        weight = 1.0f/phrases.length;
      }
      if (weights.containsKey(tokenizeTerm)) {
        weight = weights.get(tokenizeTerm) + weight;
      }
      weights.put(tokenizeTerm, weight);
    }

    return weights;
  }
  
  public double computeQLScore(Map<String, Float> queryWeights, String doc) throws IOException {
    double score = 0;
    List<String> docTerms = tokenize(ANALYZER, doc);
    //System.out.println("doc:"+docTerms.toString());
    
    int docLen = 0;
    Map<String, Integer> docTermCountMap = new HashMap<String, Integer>();
    for (String term: docTerms) {
      if (docTermCountMap.containsKey(term)) {
        docTermCountMap.put(term, docTermCountMap.get(term)+1);
      } else {
        docTermCountMap.put(term, 1);
      }
      docLen += 1;
    }

    for(String queryTerm: queryWeights.keySet()) {
      float weight = queryWeights.get(queryTerm);
      Term term = new Term(FIELD, queryTerm);
      long termFreqInCorpus = reader.totalTermFreq(term);
      if (termFreqInCorpus == 0) continue;
      int termFreqInDoc = docTermCountMap.containsKey(queryTerm) ? docTermCountMap.get(queryTerm) : 0;
      score += weight * Math.log((termFreqInDoc + mu*((double)termFreqInCorpus/reader.getSumTotalTermFreq(FIELD)))
          / (docLen + mu));
      //System.out.println("term: " + queryTerm + " freq in doc: " + termFreqInDoc 
      //    + " freq in corpus: " + termFreqInCorpus);
    }
    return score;
  }

  private static List<String> tokenize(Analyzer analyzer, String text) {
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
}
