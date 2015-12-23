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
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import java.io.*;

/**
 * Abstraction for a corpus of statuses. A corpus is assumed to consist of a number of blocks, each
 * represented by a bz2 file within a tar file. This object will allow to caller to read
 * through all blocks.
 */
public class TarJsonStatusCorpusReader implements StatusStream {
  private final TarArchiveInputStream tarInput;
  private Bz2JsonStatusBlockReader currentBlock = null;

  public TarJsonStatusCorpusReader(File file) throws IOException {
    Preconditions.checkNotNull(file);

    if (!file.isFile()) {
      throw new IOException("Expecting " + file + " to be a file!");
    }

    tarInput = new TarArchiveInputStream(new BufferedInputStream(new FileInputStream(file)));
  }

  /**
   * Returns the next status, or <code>null</code> if no more statuses.
   */
  public Status next() throws IOException {
    if (currentBlock == null) {
      // Move to next file.
      TarArchiveEntry entry = tarInput.getNextTarEntry();
      if (entry != null) {
        if (entry.getName().endsWith(".bz2")) {
          currentBlock = new Bz2JsonStatusBlockReader(tarInput);
        }
      } else {
        return null;
      }
    }

    Status status = null;
    while (true) {
      if (currentBlock != null) {
        status = currentBlock.next();
      }

      if (status != null) {
        return status;
      }

      // Move to next file.
      try {
        TarArchiveEntry entry = tarInput.getNextTarEntry();
        if (entry != null) {
          if (entry.getName().endsWith(".bz2")) {
            currentBlock = new Bz2JsonStatusBlockReader(tarInput);
          }
        } else {
          return null;
        }
      } catch (IOException e) {
        currentBlock = null;
      }
    }
  }

  public void close() throws IOException {
    currentBlock.close();
  }
}
