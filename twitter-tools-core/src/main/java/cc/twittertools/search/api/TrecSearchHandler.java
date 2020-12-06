/**
 * Twitter Tools
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cc.twittertools.search.api;

import java.io.IOException;
import java.util.*;

import javax.annotation.Nullable;

import cc.twittertools.util.AnalyzerUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;

import cc.twittertools.index.IndexStatuses;
import cc.twittertools.index.IndexStatuses.StatusField;
import cc.twittertools.thrift.gen.TQuery;
import cc.twittertools.thrift.gen.TResult;
import cc.twittertools.thrift.gen.TrecSearch;
import cc.twittertools.thrift.gen.TrecSearchException;
import cc.twittertools.util.QueryLikelihoodModel;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class TrecSearchHandler implements TrecSearch.Iface {
  private static final Logger LOG = Logger.getLogger(TrecSearchHandler.class);

  private static QueryParser QUERY_PARSER =
      new QueryParser(StatusField.TEXT.name, IndexStatuses.ANALYZER);

  private final IndexSearcher searcher;
  private final QueryLikelihoodModel qlModel;
  private final Map<String, String> credentials;

  public TrecSearchHandler(IndexSearcher searcher, QueryLikelihoodModel qlModel, @Nullable Map<String, String> credentials)
      throws IOException {
    Preconditions.checkNotNull(searcher);
    this.searcher = searcher;
    this.qlModel = qlModel;

    // Can be null, in which case we don't check for credentials.
    this.credentials = credentials;
  }

  public List<TResult> search(TQuery query) throws TrecSearchException {
    Preconditions.checkNotNull(query);

    LOG.info(String.format("Incoming request (%s, %s)", query.group, query.token));

    // Verify credentials.
    if (credentials != null && (!credentials.containsKey(query.group) ||
        !credentials.get(query.group).equals(query.token))) {
      LOG.info(String.format("Access denied for (%s, %s)", query.group, query.token));
      throw new TrecSearchException("Invalid credentials: access denied.");
    }

    List<TResult> results = Lists.newArrayList();
    long startTime = System.currentTimeMillis();

    try {
      Query filter =
          LongPoint.newRangeQuery(StatusField.ID.name, 0L, query.max_id);

      Query q = QUERY_PARSER.parse(query.text);
      Map<String, Float> weights = null;
      HashMap<String, Long> ctfs = null;
      long sumTotalTermFreq = -1;
      if (query.ql) {
        IndexReader reader = searcher.getIndexReader();
        weights = qlModel.parseQuery(IndexStatuses.ANALYZER, query.text);
        ctfs = new HashMap<String, Long>();
        for(String queryTerm: weights.keySet()) {
          Term term = new Term(IndexStatuses.StatusField.TEXT.name, queryTerm);
          long ctf = reader.totalTermFreq(term);
          ctfs.put(queryTerm, ctf);
        }
        sumTotalTermFreq = reader.getSumTotalTermFreq(IndexStatuses.StatusField.TEXT.name);
      }

      int num = query.num_results > 10000 ? 10000 : query.num_results;

      BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
      queryBuilder.add(q, BooleanClause.Occur.SHOULD);
      queryBuilder.add(filter, BooleanClause.Occur.FILTER);
      Query finalQuery = queryBuilder.build();

      TopDocs rs = searcher.search(finalQuery, num);
      for (ScoreDoc scoreDoc : rs.scoreDocs) {
        Document hit = searcher.doc(scoreDoc.doc);

        TResult p = new TResult();
        p.id = (Long) hit.getField(StatusField.ID.name).numericValue();
        p.screen_name = hit.get(StatusField.SCREEN_NAME.name);
        p.epoch = (Long) hit.getField(StatusField.EPOCH.name).numericValue();
        p.text = hit.get(StatusField.TEXT.name);
        if (query.ql) {
          List<String> docTerms = AnalyzerUtils.analyze(IndexStatuses.ANALYZER, p.text);
          //System.out.println("doc:"+docTerms.toString());

          Map<String, Integer> docVector = new HashMap<String, Integer>();
          for (String t : docTerms) {
            if (!t.isEmpty()) {
              Integer n = docVector.get(t);
              n = (n == null) ? 1 : ++n;
              docVector.put(t, n);
            }
          }
          p.rsv = qlModel.computeQLScore(weights, ctfs, docVector, sumTotalTermFreq);
        } else {
          p.rsv = scoreDoc.score;
        }

        p.followers_count = (Integer) hit.getField(StatusField.FOLLOWERS_COUNT.name).numericValue();
        p.statuses_count = (Integer) hit.getField(StatusField.STATUSES_COUNT.name).numericValue();

        if ( hit.get(StatusField.LANG.name) != null) {
          p.lang = hit.get(StatusField.LANG.name);
        }

        if ( hit.get(StatusField.IN_REPLY_TO_STATUS_ID.name) != null) {
          p.in_reply_to_status_id = (Long) hit.getField(StatusField.IN_REPLY_TO_STATUS_ID.name).numericValue();
        }

        if ( hit.get(StatusField.IN_REPLY_TO_USER_ID.name) != null) {
          p.in_reply_to_user_id = (Long) hit.getField(StatusField.IN_REPLY_TO_USER_ID.name).numericValue();
        }

        if ( hit.get(StatusField.RETWEETED_STATUS_ID.name) != null) {
          p.retweeted_status_id = (Long) hit.getField(StatusField.RETWEETED_STATUS_ID.name).numericValue();
        }

        if ( hit.get(StatusField.RETWEETED_USER_ID.name) != null) {
          p.retweeted_user_id = (Long) hit.getField(StatusField.RETWEETED_USER_ID.name).numericValue();
        }

        if ( hit.get(StatusField.RETWEET_COUNT.name) != null) {
          p.retweeted_count = (Integer) hit.getField(StatusField.RETWEET_COUNT.name).numericValue();
        }

        results.add(p);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new TrecSearchException(e.getMessage());
    }

    if (query.ql) {
      Comparator<TResult> comparator = new TResultComparator();
      Collections.sort(results, comparator);
    }

    long endTime = System.currentTimeMillis();
    LOG.info(String.format("%4dms %s", (endTime - startTime), query.toString()));

    return results;
  }

  private static class TResultComparator implements Comparator<TResult> {
    @Override
    public int compare(TResult t1, TResult t2) {
      double diff = t1.rsv - t2.rsv;
      return (diff == 0) ? 0 : (diff > 0) ? -1 : 1;
    }
  }
}