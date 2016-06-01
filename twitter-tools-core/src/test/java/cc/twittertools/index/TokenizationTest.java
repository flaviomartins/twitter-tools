package cc.twittertools.index;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import com.google.common.collect.Lists;
import org.apache.lucene.analysis.util.CharArraySet;
import org.junit.Assert;

public class TokenizationTest extends TestCase {

  Object[][] examples = new Object[][] {
      {"AT&T getting secret immunity from wiretapping laws for government surveillance http://vrge.co/ZP3Fx5",
       new String[] {"att", "get", "secret", "immun", "from", "wiretap", "law", "for", "govern", "surveil", "http://vrge.co/ZP3Fx5"}},

      {"want to see the @verge aston martin GT4 racer tear up long beach? http://theracersgroup.kinja.com/watch-an-aston-martin-vantage-gt4-tear-around-long-beac-479726219 …",
       new String[] {"want", "to", "see", "the", "@verge", "aston", "martin", "gt4", "racer", "tear", "up", "long", "beach", "http://theracersgroup.kinja.com/watch-an-aston-martin-vantage-gt4-tear-around-long-beac-479726219"}},

      {"Incredibly good news! #Drupal users rally http://bit.ly/Z8ZoFe  to ensure blind accessibility contributor gets to @DrupalCon #Opensource",
       new String[] {"incred", "good", "new", "#drupal", "user", "ralli", "http://bit.ly/Z8ZoFe", "to", "ensur", "blind", "access", "contributor", "get", "to", "@drupalcon", "#opensource"}},

      {"We're entering the quiet hours at #amznhack. #Rindfleischetikettierungsüberwachungsaufgabenübertragungsgesetz",
       new String[] {"were", "enter", "the", "quiet", "hour", "at", "#amznhack", "#rindfleischetikettierungsüberwachungsaufgabenübertragungsgesetz"}},

      {"The 2013 Social Event Detection Task (SED) at #mediaeval2013, http://bit.ly/16nITsf  supported by @linkedtv @project_mmixer @socialsensor_ip",
       new String[] {"the", "2013", "social", "event", "detect", "task", "sed", "at", "#mediaeval2013", "http://bit.ly/16nITsf", "support", "by", "@linkedtv", "@project_mmixer", "@socialsensor_ip"}},

      {"U.S.A. U.K. U.K USA UK #US #UK #U.S.A #U.K ...A.B.C...D..E..F..A.LONG WORD",
       new String[] {"usa", "uk", "uk", "usa", "uk", "#us", "#uk", "#u", "sa", "#u", "k", "abc", "d", "e", "f", "a", "long", "word"}},

      {"this is @a_valid_mention and this_is_multiple_words",
       new String[] {"thi", "is", "@a_valid_mention", "and", "thi", "is", "multipl", "word"}},

      {"PLEASE BE LOWER CASE WHEN YOU COME OUT THE OTHER SIDE - ALSO A @VALID_VALID-INVALID",
       new String[] {"pleas", "be", "lower", "case", "when", "you", "come", "out", "the", "other", "side", "also", "a", "@valid_valid", "invalid"}},

      // Note: the at sign is not the normal (at) sign and the crazy hashtag is not the normal #
      {"＠reply @with #crazy ~＃at",
       new String[] {"＠reply", "@with", "#crazy", "＃at"}},

      {":@valid testing(valid)#hashtags. RT:@meniton (the last @mention is #valid and so is this:@valid), however this is@invalid",
       new String[] {"@valid", "test", "valid", "#hashtags", "rt", "@meniton", "the", "last", "@mention", "is", "#valid", "and", "so", "is", "thi", "@valid", "howev", "thi", "is", "invalid"}},

      {"this][is[lots[(of)words+with-lots=of-strange!characters?$in-fact=it&has&Every&Single:one;of<them>in_here_B&N_test_test?test\\test^testing`testing{testing}testing…testing¬testing·testing what?",
       new String[] {"thi", "is", "lot", "of", "word", "with", "lot", "of", "strang", "charact", "in", "fact", "it", "ha", "everi", "singl", "on", "of", "them", "in", "here", "bn", "test", "test", "test", "test", "test", "test", "test", "test", "test", "test", "test", "what"}},

      {"@Porsche : 2014 is already here #zebracar #LM24 http://bit.ly/18RUczp\u00a0 pic.twitter.com/cQ7z0c2hMg",
       new String[] {"@porsche", "2014", "is", "alreadi", "here", "#zebracar", "#lm24", "http://bit.ly/18RUczp", "pic.twitter.com/cQ7z0c2hMg"}},

      {"Some cars are in the river #NBC4NY http://t.co/WmK9Hc…",
       new String[] {"some", "car", "ar", "in", "the", "river", "#nbc4ny", "http://t.co/WmK9Hc"}},

      {"“@mention should be detected",
       new String[] {"@mention", "should", "be", "detect"}},

      {"Mr. Rogers's shows",
       new String[] {"mr", "roger", "show"}},

      {"'Oz, The Great and Powerful' opens",
       new String[] {"oz", "the", "great", "and", "power", "open"}}
  };

  public void testTokenizer() throws Exception {
    Analyzer analyzer = new TweetAnalyzer(CharArraySet.EMPTY_SET);

    for (int i = 0; i < examples.length; i++) {
      Assert.assertEquals(
              Arrays.toString((String[]) examples[i][1]),
              Arrays.toString(analyze(analyzer, (String) examples[i][0])));
    }
  }

  public String[] analyze(Analyzer analyzer, String text) throws IOException {
    List<String> list = Lists.newArrayList();

    TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(text));
    CharTermAttribute cattr = tokenStream.addAttribute(CharTermAttribute.class);
    tokenStream.reset();
    while (tokenStream.incrementToken()) {
      String term = cattr.toString();
      list.add(term);
    }
    tokenStream.end();
    tokenStream.close();

    return list.toArray(new String[list.size()]);
  }

}
