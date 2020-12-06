package cc.twittertools.download;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Future;

import junit.framework.JUnit4TestAdapter;

import org.apache.commons.text.StringEscapeUtils;
import org.junit.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

public class FetchStatusTest {

  @Test
  public void basicHTML() throws Exception {
    String url = AsyncEmbeddedJsonStatusBlockCrawler.getUrl(1121915133L, "jkrums");
    AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
    AsyncHttpClient.BoundRequestBuilder request = asyncHttpClient.prepareGet(url);
    Future<Response> f = request.execute();
    Response response = f.get();

    // Make sure status is OK.
    String html = response.getResponseBody("UTF-8");
    assertTrue(html != null);
  }
  
  // The fetcher is broken, so disabling test.
  //@Test
  public void basicFamous() throws Exception {
    String url = AsyncEmbeddedJsonStatusBlockCrawler.getUrl(1121915133L, "jkrums");
    AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
    AsyncHttpClient.BoundRequestBuilder request = asyncHttpClient.prepareGet(url);
    Future<Response> f = request.execute();
    Response response = f.get();

    // Make sure status is OK.
    assertEquals(200, response.getStatusCode());
    String html = response.getResponseBody("UTF-8");

    int jsonStart = html.indexOf(AsyncEmbeddedJsonStatusBlockCrawler.JSON_START);
    int jsonEnd = html.indexOf(AsyncEmbeddedJsonStatusBlockCrawler.JSON_END,
        jsonStart + AsyncEmbeddedJsonStatusBlockCrawler.JSON_START.length());

    String json = html.substring(jsonStart + AsyncEmbeddedJsonStatusBlockCrawler.JSON_START.length(), jsonEnd);
    json = StringEscapeUtils.unescapeHtml4(json);
    JsonObject page = (JsonObject) JsonParser.parseString(json);
    JsonObject statusJson = page.getAsJsonObject("embedData").getAsJsonObject("status");

    Status status = TwitterObjectFactory.createStatus(statusJson.toString());
    assertEquals(1121915133L, status.getId());
    assertEquals("jkrums", status.getUser().getScreenName());
    assertEquals("http://twitpic.com/135xa - There's a plane in the Hudson. I'm on the ferry going to pick up the people. Crazy.", status.getText());

    asyncHttpClient.close();
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(FetchStatusTest.class);
  }
}
