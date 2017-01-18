package cc.twittertools.corpus.data;

import twitter4j.Status;

import java.io.IOException;
import java.util.Random;

public class SampledStatusStream implements StatusStream {

  private final StatusStream stream;
  private final float sample;
  private final Random random;

  public SampledStatusStream(StatusStream stream, float samplePercentage) {
    this.stream = stream;
    this.sample = samplePercentage/100.0f;
    this.random = new Random();
  }

  @Override
  public Status next() throws IOException {
    Status next;
    do {
      next = stream.next();
    } while (next != null && random.nextFloat() >= sample);
    return next;
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }
}
