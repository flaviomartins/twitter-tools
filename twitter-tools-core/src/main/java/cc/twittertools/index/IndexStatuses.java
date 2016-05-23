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

package cc.twittertools.index;

import cc.twittertools.corpus.data.TwitterstreamJsonStatusCorpusReader;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.Version;

import cc.twittertools.corpus.data.JsonStatusCorpusReader;
import cc.twittertools.corpus.data.StatusStream;

/**
 * Reference implementation for indexing statuses.
 */
public class IndexStatuses {
  private static final Logger LOG = Logger.getLogger(IndexStatuses.class);

  public static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);

  private IndexStatuses() {}

  public enum StatusField {
    ID("id"),
    SCREEN_NAME("screen_name"),
    EPOCH("epoch"),
    TEXT("text"),
    LANG("lang"),
    IN_REPLY_TO_STATUS_ID("in_reply_to_status_id"),
    IN_REPLY_TO_USER_ID("in_reply_to_user_id"),
    FOLLOWERS_COUNT("followers_count"),
    FRIENDS_COUNT("friends_count"),
    STATUSES_COUNT("statuses_count"),
    RETWEETED_STATUS_ID("retweeted_status_id"),
    RETWEETED_USER_ID("retweeted_user_id"),
    RETWEET_COUNT("retweet_count");

    public final String name;

    StatusField(String s) {
      name = s;
    }
  }

  private static final String HELP_OPTION = "h";
  private static final String COLLECTION_OPTION = "collection";
  private static final String INDEX_OPTION = "index";
  private static final String MAX_ID_OPTION = "max_id";
  private static final String DELETES_OPTION = "deletes";
  private static final String OPTIMIZE_OPTION = "optimize";
  private static final String STORE_TERM_VECTORS_OPTION = "store";
  private static final String VERBOSE_OPTION = "verbose";
  private static final String UPDATE_OPTION = "update";
  private static final String PRINT_DPS_OPTION = "printDPS";
  private static final String THREAD_COUNT_OPTION = "threadCount";
  private static final String FORMAT_OPTION = "twitterstream";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(new Option(HELP_OPTION, "show help"));
    options.addOption(new Option(OPTIMIZE_OPTION, "merge indexes into a single segment"));
    options.addOption(new Option(STORE_TERM_VECTORS_OPTION, "store term vectors"));
    options.addOption(new Option(VERBOSE_OPTION, "more verbose"));
    options.addOption(new Option(UPDATE_OPTION, "update index"));
    options.addOption(new Option(PRINT_DPS_OPTION, "print DPS"));
    options.addOption(new Option(FORMAT_OPTION, "use twitterstream tar format"));

    options.addOption(OptionBuilder.withArgName("dir").hasArg()
        .withDescription("source collection directory").create(COLLECTION_OPTION));
    options.addOption(OptionBuilder.withArgName("dir").hasArg()
        .withDescription("index location").create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("file").hasArg()
        .withDescription("file with deleted tweetids").create(DELETES_OPTION));
    options.addOption(OptionBuilder.withArgName("id").hasArg()
        .withDescription("max id").create(MAX_ID_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
            .withDescription("number of threads").create(THREAD_COUNT_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (cmdline.hasOption(HELP_OPTION) || !cmdline.hasOption(COLLECTION_OPTION)
        || !cmdline.hasOption(INDEX_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(IndexStatuses.class.getName(), options);
      System.exit(-1);
    }

    String dataDir = cmdline.getOptionValue(COLLECTION_OPTION);
    String dirPath = cmdline.getOptionValue(INDEX_OPTION);
    boolean forceMerge = cmdline.hasOption(OPTIMIZE_OPTION);
    boolean verbose = cmdline.hasOption(VERBOSE_OPTION);
    boolean doUpdate = cmdline.hasOption(UPDATE_OPTION);
    boolean printDPS = cmdline.hasOption(PRINT_DPS_OPTION);

    int numThreads = 4;
    if (cmdline.hasOption(THREAD_COUNT_OPTION)) {
      numThreads = Integer.parseInt(cmdline.getOptionValue(THREAD_COUNT_OPTION));
    }

    final FieldType textOptions = new FieldType();
    textOptions.setIndexed(true);
    textOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    textOptions.setStored(true);
    textOptions.setTokenized(true);
    if (cmdline.hasOption(STORE_TERM_VECTORS_OPTION)) {
      textOptions.setStoreTermVectors(true);
    }

    LongOpenHashSet deletes = null;
    if (cmdline.hasOption(DELETES_OPTION)) {
      deletes = new LongOpenHashSet();
      File deletesFile = new File(cmdline.getOptionValue(DELETES_OPTION));
      if (!deletesFile.exists()) {
        System.err.println("Error: " + deletesFile + " does not exist!");
        System.exit(-1);
      }
      LOG.info("Reading deletes from " + deletesFile);
      
      FileInputStream fin = new FileInputStream(deletesFile);
      BufferedInputStream bis = new BufferedInputStream(fin);
      CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
      BufferedReader br = new BufferedReader(new InputStreamReader(input));

      String s;
      while ((s = br.readLine()) != null) {
        if (s.contains("\t")) {
          deletes.add(Long.parseLong(s.split("\t")[0]));
        } else {
          deletes.add(Long.parseLong(s));
        }
      }
      br.close();
      fin.close();
      LOG.info("Read " + deletes.size() + " tweetids from deletes file.");
    }

    long maxId = Long.MAX_VALUE;
    if (cmdline.hasOption(MAX_ID_OPTION)) {
      maxId = Long.parseLong(cmdline.getOptionValue(MAX_ID_OPTION));
      LOG.info("index: " + maxId);
    }

    long docCountLimit = -1;
    
    File file = new File(dataDir);
    if (!file.exists()) {
      System.err.println("Error: " + file + " does not exist!");
      System.exit(-1);
    }

    StatusStream stream;
    if (cmdline.hasOption(FORMAT_OPTION)) {
      LOG.info("format: use twitterstream tar format");
      stream = new TwitterstreamJsonStatusCorpusReader(file, numThreads);
    } else {
      stream = new JsonStatusCorpusReader(file);
    }

    final Directory dir = FSDirectory.open(new File(dirPath));

    LOG.info("Index path: " + dirPath);
    LOG.info("Threads: " + numThreads);
    LOG.info("Verbose: " + (verbose ? "yes" : "no"));
    LOG.info("Force merge: " + (forceMerge ? "yes" : "no"));

    if (verbose) {
      InfoStream.setDefault(new PrintStreamInfoStream(System.out));
    }

    final IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_43, IndexStatuses.ANALYZER);
    // More RAM before flushing means Lucene writes larger segments to begin with which means less merging later.
    iwc.setRAMBufferSizeMB(48);

    if (doUpdate) {
      iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
    } else {
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    }
    if (forceMerge) {
      iwc.setMergePolicy(new TieredMergePolicy());
    }
    LOG.info("IW config=" + iwc);

    final IndexWriter w = new IndexWriter(dir, iwc);
    IndexThreads threads = new IndexThreads(w, textOptions, stream, numThreads, -1, printDPS, maxId, deletes);
    LOG.info("Indexer: start");

    final long t0 = System.currentTimeMillis();

    threads.start();

    while (!threads.done()) {
      Thread.sleep(100);
    }
    threads.stop();

    final long t1 = System.currentTimeMillis();
    LOG.info("Indexer: indexing done (" + (t1-t0)/1000.0 + " sec); total " + w.maxDoc() + " docs");
    if (!doUpdate && docCountLimit != -1 && w.maxDoc() != docCountLimit) {
      throw new RuntimeException("w.maxDoc()=" + w.maxDoc() + " but expected " + docCountLimit);
    }
    if (threads.failed.get()) {
      throw new RuntimeException("exceptions during indexing");
    }


    final long t2;
    t2 = System.currentTimeMillis();

    final Map<String,String> commitData = new HashMap<String,String>();
    commitData.put("userData", "multi");
    w.setCommitData(commitData);
    w.commit();
    final long t3 = System.currentTimeMillis();
    LOG.info("Indexer: commit multi (took " + (t3-t2)/1000.0 + " sec)");


    if (forceMerge) {
      LOG.info("Starting the merge...");
      long mergeStart = System.currentTimeMillis();
      w.forceMerge(1);
      w.commit();
      LOG.info("Indexer: merging took " + (System.currentTimeMillis() - mergeStart)/1000.0 + " sec");
    }
    LOG.info("Indexer: at close: " + w.segString());
    final long tCloseStart = System.currentTimeMillis();
    w.close();
    LOG.info("Indexer: close took " + (System.currentTimeMillis() - tCloseStart)/1000.0 + " sec");
    dir.close();
    final long tFinal = System.currentTimeMillis();
    LOG.info("Indexer: finished (" + (tFinal-t0)/1000.0 + " sec)");
  }
}
