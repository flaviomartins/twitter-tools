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
import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * Abstraction for an stream of statuses, backed by an underlying bz2 file with JSON-encoded
 * tweets, one per line.
 */
public class Bz2JsonStatusBlockReader implements StatusStream {

  private static final Object POISON_PILL = new Object();

  private final InputStream in;
  private final BufferedReader br;
  private final CountDownLatch stopLatch;
  private final BlockingQueue blockingQueue;

  public Bz2JsonStatusBlockReader(InputStream in, final ExecutorService executor) throws IOException {
    this.in = in;
    Preconditions.checkNotNull(in);

    br = new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(in), "UTF-8"));

    blockingQueue = new ArrayBlockingQueue(100000);

    ArrayList<String> read = new ArrayList<String>();
    String raw;
    // Check to see if we've reached end of file.
    while ((raw = br.readLine()) != null) {
      read.add(raw);
    }

    stopLatch = new CountDownLatch(read.size());

    for (String line : read) {
      Runnable worker = new JsonReaderThread(stopLatch, line, blockingQueue);
      executor.execute(worker);
    }

    // waits for the tasks to finish or timeout
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          stopLatch.await();
          blockingQueue.put(POISON_PILL);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }).start();
  }

  static class JsonReaderThread implements Runnable {

    private final CountDownLatch stopLatch;
    private final String raw;
    private final BlockingQueue blockingQueue;

    public JsonReaderThread(CountDownLatch stopLatch, String raw, BlockingQueue blockingQueue) {
      this.stopLatch = stopLatch;
      this.raw = raw;
      this.blockingQueue = blockingQueue;
    }

    @Override
    public void run() {
      try {
        Status nxt = TwitterObjectFactory.createStatus(raw);
        blockingQueue.put(nxt);
      } catch (TwitterException e) {
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        stopLatch.countDown();
      }
    }
  }

  /**
   * Returns the next status, or <code>null</code> if no more statuses.
   */
  public Status next() throws IOException {
    try {
      Object element = blockingQueue.take();
      if (POISON_PILL.equals(element))
        return null;

      return (Status) element;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void close() throws IOException {
    br.close();
  }
}
