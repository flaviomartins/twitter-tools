package cc.twittertools.index;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import cc.twittertools.corpus.data.StatusStream;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.log4j.Logger;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import twitter4j.Status;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class IndexThreads {
  private static final Logger LOG = Logger.getLogger(IndexThreads.class);

  private static final Object POISON_PILL = new Object();

  final IngestRatePrinter printer;
  final CountDownLatch startLatch = new CountDownLatch(1);
  final AtomicBoolean stop;
  final AtomicBoolean failed;
  final StatusStream stream;
  final Thread[] threads;
  final BlockingQueue<Status> blockingQueue;
  final StatusProducer statusProducer;

  public IndexThreads(IndexWriter w, FieldType textOptions,
                      StatusStream stream, int numThreads, int docCountLimit, boolean printDPS, long maxId, LongOpenHashSet deletes) throws IOException, InterruptedException {

    this.stream = stream;
    threads = new Thread[numThreads];

    final CountDownLatch stopLatch = new CountDownLatch(numThreads);
    final AtomicInteger prodCount = new AtomicInteger();
    final AtomicInteger docCount = new AtomicInteger();
    stop = new AtomicBoolean(false);
    failed = new AtomicBoolean(false);

    blockingQueue = new ArrayBlockingQueue<Status>(100000);
    statusProducer = new StatusProducer(blockingQueue, stream, numThreads, maxId, deletes, prodCount, stop);

    for (int thread = 0; thread < numThreads; thread++) {
      threads[thread] = new IndexThread(startLatch, stopLatch, w, textOptions, blockingQueue, docCountLimit, docCount, stop, failed);
      threads[thread].start();
    }

    Thread.sleep(10);

    statusProducer.start();

    if (printDPS) {
      printer = new IngestRatePrinter(prodCount, docCount, stop);
      printer.start();
    } else {
      printer = null;
    }
  }

  public void start() {
    startLatch.countDown();
  }

  public void stop() throws InterruptedException, IOException {
    stop.getAndSet(true);
    for (Thread t : threads) {
      t.join();
    }
    if (printer != null) {
      printer.join();
    }
    stream.close();
  }

  public boolean done() {
    for (Thread t : threads) {
      if (t.isAlive()) {
        return false;
      }
    }

    return true;
  }

  private static class IndexThread extends Thread {
    private final BlockingQueue blockingQueue;
    private final int numTotalDocs;
    private final IndexWriter w;
    private final FieldType textOptions;
    private final AtomicInteger count;
    private final CountDownLatch startLatch;
    private final CountDownLatch stopLatch;
    private final AtomicBoolean failed;

    public IndexThread(CountDownLatch startLatch, CountDownLatch stopLatch, IndexWriter w, FieldType textOptions,
                       BlockingQueue blockingQueue, int numTotalDocs, AtomicInteger count,
                       AtomicBoolean stop, AtomicBoolean failed) {
      this.startLatch = startLatch;
      this.stopLatch = stopLatch;
      this.w = w;
      this.textOptions = textOptions;
      this.blockingQueue = blockingQueue;
      this.numTotalDocs = numTotalDocs;
      this.count = count;
      this.failed = failed;
    }

    private Document getDocumentFromDocData(Status status) {
      Document doc = new Document();
      long id = status.getId();
      doc.add(new LongField(IndexStatuses.StatusField.ID.name, id, Field.Store.YES));
      doc.add(new LongField(IndexStatuses.StatusField.EPOCH.name, status.getCreatedAt().getTime() / 1000L, Field.Store.YES));
      doc.add(new TextField(IndexStatuses.StatusField.SCREEN_NAME.name, status.getUser().getScreenName(), Field.Store.YES));

      doc.add(new Field(IndexStatuses.StatusField.TEXT.name, status.getText(), textOptions));

      doc.add(new IntField(IndexStatuses.StatusField.FRIENDS_COUNT.name, status.getUser().getFriendsCount(), Field.Store.YES));
      doc.add(new IntField(IndexStatuses.StatusField.FOLLOWERS_COUNT.name, status.getUser().getFollowersCount(), Field.Store.YES));
      doc.add(new IntField(IndexStatuses.StatusField.STATUSES_COUNT.name, status.getUser().getStatusesCount(), Field.Store.YES));

      long inReplyToStatusId = status.getInReplyToStatusId();
      if (inReplyToStatusId > 0) {
        doc.add(new LongField(IndexStatuses.StatusField.IN_REPLY_TO_STATUS_ID.name, inReplyToStatusId, Field.Store.YES));
        doc.add(new LongField(IndexStatuses.StatusField.IN_REPLY_TO_USER_ID.name, status.getInReplyToUserId(), Field.Store.YES));
      }

      String lang = status.getLang();
      if (lang != null && !"unknown".equals(lang)) {
        doc.add(new TextField(IndexStatuses.StatusField.LANG.name, lang, Field.Store.YES));
      }

      // In Tweets2011 retweet_count exists but not the other retweet fields
      int retweetCount = status.getRetweetCount();
      if (retweetCount > -1) {
        doc.add(new IntField(IndexStatuses.StatusField.RETWEET_COUNT.name, retweetCount, Field.Store.YES));
      }

      Status retweetedStatus = status.getRetweetedStatus();
      if (retweetedStatus != null) {
        doc.add(new LongField(IndexStatuses.StatusField.RETWEETED_STATUS_ID.name, status.getRetweetedStatus().getId(), Field.Store.YES));
        doc.add(new LongField(IndexStatuses.StatusField.RETWEETED_USER_ID.name, status.getRetweetedStatus().getUser().getId(), Field.Store.YES));

        if (retweetCount < 0) {
          LOG.warn("Error parsing retweet fields of " + id);
        }
      }
      return doc;
    }

    @Override
    public void run() {
      try {
        final long tStart = System.currentTimeMillis();

        try {
          startLatch.await();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }

        while (true) {
          Object element = blockingQueue.take();
          if (POISON_PILL.equals(element))
            break;

          Status status = (Status) element;

          Document doc = getDocumentFromDocData(status);
          if (doc == null) {
            break;
          }
          int docCount = count.incrementAndGet();
          if (numTotalDocs != -1 && docCount > numTotalDocs) {
            break;
          }
          if ((docCount % 100000) == 0) {
            LOG.info("Indexer: " + docCount + " docs... (" + (System.currentTimeMillis() - tStart) / 1000.0 + " sec)");
          }
          w.addDocument(doc);
        }

      } catch (Exception e) {
        failed.set(true);
        throw new RuntimeException(e);
      } finally {
        stopLatch.countDown();
      }
    }
  }

  private static class StatusProducer extends Thread {

    private final StatusStream stream;
    private final BlockingQueue blockingQueue;
    private final int numThreads;
    private final long maxId;
    private final LongOpenHashSet deletes;
    private final AtomicInteger count;
    private final AtomicBoolean stop;

    public StatusProducer(BlockingQueue blockingQueue, StatusStream stream, int numThreads, long maxId, LongOpenHashSet deletes, AtomicInteger count, AtomicBoolean stop) {
      this.blockingQueue = blockingQueue;
      this.stream = stream;
      this.numThreads = numThreads;
      this.maxId = maxId;
      this.deletes = deletes;
      this.count = count;
      this.stop = stop;
    }

    @Override
    public void run() {
      long start = System.currentTimeMillis();
      LOG.info("Producer: start");

      while (!stop.get()) {
        try {
          Status status = stream.next();
          if (status == null)
            break;

          if (status.getText() == null) {
            continue;
          }

          // Skip deletes tweetids.
          if (deletes != null && deletes.contains(status.getId())) {
            continue;
          }

          if (status.getId() > maxId) {
            continue;
          }

          blockingQueue.put(status);
          count.incrementAndGet();
        } catch (Exception ex) {
        }
      }

      try {
        for (int thread = 0; thread < numThreads; thread++) {
          blockingQueue.put(POISON_PILL);
        }
      } catch (Exception ex) {
      }

      int numProd = count.get();
      long now = System.currentTimeMillis();
      double seconds = (now - start) / 1000.0d;
      LOG.info("Producer: " + (int)(numProd / seconds) + " " + seconds + " " + numProd + " produced...");
    }
  }

  private static class IngestRatePrinter extends Thread {

    private final AtomicInteger prodCount;
    private final AtomicInteger docCount;
    private final AtomicBoolean stop;

    public IngestRatePrinter(AtomicInteger prodCount, AtomicInteger docCount, AtomicBoolean stop) {
      this.prodCount = prodCount;
      this.docCount = docCount;
      this.stop = stop;
    }

    @Override
    public void run() {
      long time = System.currentTimeMillis();
      LOG.info("startIngest: " + time);
      final long start = time;
      int lastProdCount = prodCount.get();
      int lastDocsCount = docCount.get();
      while (!stop.get()) {
        try {
          Thread.sleep(200);
        } catch (Exception ex) {
        }
        int numProd = prodCount.get();
        int numDocs = docCount.get();

        double currentProd = numProd - lastProdCount;
        double currentDocs = numDocs - lastDocsCount;
        long now = System.currentTimeMillis();
        double seconds = (now - time) / 1000.0d;
        LOG.info("ingest: " + (now - start) + " " + (int)(currentProd / seconds) + " " + (int)(currentDocs / seconds) + " " + numProd + " " + numDocs);
        time = now;
        lastProdCount = numProd;
        lastDocsCount = numDocs;
      }
    }
  }
}
