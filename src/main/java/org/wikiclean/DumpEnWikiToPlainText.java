/**
 * WikiClean: A Java Wikipedia markup to plain text converter
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

package org.wikiclean;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import org.wikiclean.WikiClean.WikiLanguage;

import java.io.File;
import java.io.PrintWriter;

/**
 * Simple program for dumping English Wikipedia articles to plain text, one article per line.
 */
public class DumpEnWikiToPlainText {
  private DumpEnWikiToPlainText() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    File input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;
  }

  public static void main(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.exit(-1);
    }

    final WikiClean cleaner = new WikiClean.Builder().withLanguage(WikiLanguage.EN)
        .withTitle(false).withFooter(false).build();

    PrintWriter writer = new PrintWriter(args.output, "UTF-8");
    WikipediaArticlesDump wikipedia = new WikipediaArticlesDump(args.input, null);

    wikipedia.stream()
        .filter(page -> !page.contains("<ns>") || page.contains("<ns>0</ns>"))
        .forEach(page -> {
          String s = cleaner.clean(page).replaceAll("\\n+", " ");
          if (s.startsWith("#REDIRECT")) return;

          writer.println(cleaner.getTitle(page).replaceAll("\\n+", " ") + "\t" + s);
        });

    writer.close();
  }
}
