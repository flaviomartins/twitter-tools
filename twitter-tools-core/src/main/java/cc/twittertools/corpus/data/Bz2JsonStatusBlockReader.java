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
import twitter4j.TwitterObjectFactory;

import java.io.*;
import java.util.concurrent.*;

/**
 * Abstraction for an stream of statuses, backed by an underlying bz2 file with JSON-encoded
 * tweets, one per line.
 */
public class Bz2JsonStatusBlockReader implements StatusStream {

  private static final Object POISON_PILL = new Object();

  private final InputStream in;
  private final BufferedReader br;
  private final BlockingQueue blockingQueue;

  public Bz2JsonStatusBlockReader(InputStream in, final ExecutorService executor) throws IOException {
    this.in = in;
    Preconditions.checkNotNull(in);

    br = new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(in), "UTF-8"));

    blockingQueue = new ArrayBlockingQueue(100000);

    final CompletionService<Status> completionService = new ExecutorCompletionService<Status>(executor);

    int numTasks = 0;
    String raw;
    while ((raw = br.readLine()) != null) {
      completionService.submit(new JsonReaderCallable(raw));
      numTasks++;
    }

    final int tasksNumber = numTasks;

    new Thread(new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < tasksNumber; i++) {
          try {
              final Future<Status> future = completionService.take();
              blockingQueue.put(future.get());
          } catch (InterruptedException e) {
              // do nothing
          } catch (ExecutionException e) {
              // do nothing
          }
        }

        try {
          blockingQueue.put(POISON_PILL);
        } catch (InterruptedException e) {
          // do nothing
        }
      }
    }).start();
  }

  static class JsonReaderCallable implements Callable<Status> {

    private final String raw;

    public JsonReaderCallable(String raw) {
      this.raw = raw;
    }

    @Override
    public Status call() throws Exception {
      Status nxt = TwitterObjectFactory.createStatus(raw);
      return nxt;
    }
  }

  /**
   * Returns the next status, or <code>null</code> if no more statuses.
   */
  public Status next() {
    try {
      Object element = blockingQueue.take();
      if (POISON_PILL.equals(element))
        return null;

      return (Status) element;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() throws IOException {
    br.close();
  }
}
