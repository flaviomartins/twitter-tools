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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import twitter4j.Status;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.concurrent.*;

/**
 * Abstraction for a corpus of statuses. A corpus is assumed to consist of a number of blocks, each
 * represented by a tar file within a root directory. This object will allow to caller to read
 * through all blocks, in sorted lexicographic order of the files.
 */
public class TwitterstreamJsonStatusCorpusReader implements StatusStream {

  private static final Object POISON_PILL = new Object();

  private final File[] files;
  private final BlockingQueue blockingQueue;
  private final ExecutorService innerExecutor;
  private final ExecutorService executor;

  public TwitterstreamJsonStatusCorpusReader(File file, int numThreads) throws IOException {
    Preconditions.checkNotNull(file);

    if (!file.isDirectory()) {
      throw new IOException("Expecting " + file + " to be a directory!");
    }

    files = file.listFiles(new FileFilter() {
      public boolean accept(File path) {
        return path.getName().endsWith(".tar") ? true : false;
      }
    });

    if (files.length == 0) {
      throw new IOException(file + " does not contain any .tar files!");
    }

    blockingQueue = new ArrayBlockingQueue(100000);

    innerExecutor = Executors.newFixedThreadPool(numThreads,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("TarInnerReaderThread-%d").build());

    executor = Executors.newFixedThreadPool(numThreads,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("TarReaderThread-%d").build());
    for (File tarFile : files) {
      Runnable worker = new TarReaderThread(tarFile, blockingQueue, innerExecutor);
      executor.execute(worker);
    }
    executor.shutdown();

    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
          blockingQueue.put(POISON_PILL);
        } catch (InterruptedException e) {
          // do nothing
        }
      }
    }).start();
  }

  static class TarReaderThread implements Runnable {

    private final File file;
    private final BlockingQueue blockingQueue;
    private final ExecutorService executor;
    private TarJsonStatusCorpusReader currentReader;

    public TarReaderThread(File file, BlockingQueue blockingQueue, ExecutorService executor) {
      this.file = file;
      this.blockingQueue = blockingQueue;
      this.executor = executor;
    }

    @Override
    public void run() {
      try {
        currentReader = new TarJsonStatusCorpusReader(file, executor);

        Status status;
        while ((status = currentReader.next()) != null) {
          blockingQueue.put(status);
        }
      } catch (Exception e) {
          e.printStackTrace();
      }
    }
  }

  /**
   * Returns the next status, or <code>null</code> if no more statuses.
   */
  public Status next() throws IOException {
    try {
      Object element = blockingQueue.take();
      if (POISON_PILL.equals(element)) {
          innerExecutor.shutdown();
          return null;
      }

      return (Status) element;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() throws IOException {
//    currentReader.close();
  }
}
