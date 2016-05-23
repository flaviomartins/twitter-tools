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

package cc.twittertools.corpus.data;

import com.google.common.base.Preconditions;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.*;

/**
 * Abstraction for an stream of statuses, backed by an underlying bz2 file with JSON-encoded
 * tweets, one per line.
 */
public class Bz2JsonStatusBlockReader implements StatusStream {
  private final InputStream in;
  private final BufferedReader br;

  public Bz2JsonStatusBlockReader(InputStream in) throws IOException {
    this.in = in;
    Preconditions.checkNotNull(in);

    br = new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(in), "UTF-8"));
  }

  /**
   * Returns the next status, or <code>null</code> if no more statuses.
   */
  public Status next() throws IOException {
    Status nxt = null;
    String raw = null;

    while (nxt == null) {
      raw = br.readLine();

      // Check to see if we've reached end of file.
      if (raw == null) {
        return null;
      }

      try {
        nxt = TwitterObjectFactory.createStatus(raw);
      } catch (TwitterException e) {
      }
    }
    return nxt;
  }

  public void close() throws IOException {
    br.close();
  }
}
