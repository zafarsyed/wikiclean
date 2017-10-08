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
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.ParseException;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.tools.bzip2.CBZip2InputStream;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import org.wikiclean.WikiClean.WikiLanguage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Object for reading Wikipedia articles from a bz2-compressed dump file.
 */
public class WikipediaArticlesDump implements Iterable<String> {
  private static final int DEFAULT_STRINGBUFFER_CAPACITY = 1024;

  private final BufferedReader reader;
  private final FileInputStream stream;
  private final String outputFile;

  /**
   * Class constructor.
   * @param file path to dump file
   * @throws IOException if any file-related errors are encountered
   */
  public WikipediaArticlesDump(File file, String outputFile) throws IOException {
    stream = new FileInputStream(file);
    byte[] ignoreBytes = new byte[2];
    stream.read(ignoreBytes); // "B", "Z" bytes from commandline tools
    reader = new BufferedReader(new InputStreamReader(new CBZip2InputStream(
            new BufferedInputStream(stream)), "UTF8"));
    this.outputFile = outputFile;
  }

  /**
   * Provides an iterator over Wikipedia articles.
   * @return an iterator over Wikipedia articles
   */
  public Iterator<String> iterator() {
    return new Iterator<String>() {
      private String nextArticle = null;

      public boolean hasNext() {
        if (nextArticle != null) {
          return true;
        }

        try {
          nextArticle = readNext();
        } catch (IOException e) {
          return false;
        }

        return nextArticle!= null;
      }

      public String next() {
        // If current article is null, try to advance.
        if (nextArticle == null) {
          try {
            nextArticle = readNext();
            // If we advance and and still get a null, then we're done.
            if (nextArticle == null) {
              throw new NoSuchElementException();
            }
          } catch (IOException e) {
            throw new NoSuchElementException();
          }
        }

        String article = nextArticle;
        nextArticle = null;
        return article;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }

      private String readNext() throws IOException {
        String s;
        StringBuilder sb = new StringBuilder(DEFAULT_STRINGBUFFER_CAPACITY);

        while ((s = reader.readLine()) != null) {
          if (s.endsWith("<page>"))
            break;
        }

        if (s == null) {
          stream.close();
          reader.close();
          return null;
        }

        sb.append(s).append("\n");

        while ((s = reader.readLine()) != null) {
          sb.append(s).append("\n");

          if (s.endsWith("</page>"))
            break;
        }

        return sb.toString();
      }
    };
  }

  /**
   * Provides a stream of Wikipedia articles.
   * @return a stream of Wikipedia articles
   */
  public Stream<String> stream() {
    return StreamSupport.stream(this.spliterator(), false);
  }

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    File input;

    @Option(name = "-lang", metaVar = "[lang]", usage = "two-letter language code")
    String lang = "en";
  }

  /**
   * Simple program prints out all cleaned articles.
   * @param argv command-line argument
   * @throws Exception if any errors are encountered
   */
  public static void main(String[] argv) throws Exception, IOException {
	long start = System.currentTimeMillis();
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.exit(-1);
    }

    WikiLanguage lang = WikiLanguage.EN;
    if (args.lang.equalsIgnoreCase("de")) {
      lang = WikiLanguage.DE;
    } else  if (args.lang.equalsIgnoreCase("zh")) {
      lang = WikiLanguage.ZH;
    }

    PrintStream out = new PrintStream(System.out, true, "UTF-8");
    WikiClean cleaner = new WikiClean.Builder().withLanguage(lang).build();

    WikipediaArticlesDump wikipedia = new WikipediaArticlesDump(args.input,"E://Wikipedia_articles//Wikipedia_cleanXML//enwiki-20171001-pages-meta-current1.xml-p10p30303.json");
    //RestClient restClient = RestClient.builder(new HttpHost("131.234.28.254", 9200, "http")).build();
    AtomicInteger cnt = new AtomicInteger();
    FileWriter fw = new FileWriter(wikipedia.outputFile);
    wikipedia.stream()
        // See https://en.wikipedia.org/wiki/Wikipedia:Namespace
        .filter(s -> !s.contains("<ns>") || s.contains("<ns>0</ns>"))
        .forEach(s -> {
          //out.println("Title = " + cleaner.getTitle(s));
          //out.println("Id = " + cleaner.getId(s));
          //out.println(cleaner.clean(s) + "\n\n#################################\n");
          ObjectMapper mapper = new ObjectMapper();
  			ObjectNode objectNode1 = mapper.createObjectNode();
          objectNode1.put("Title", cleaner.getTitle(s));
          objectNode1.put("Article", cleaner.clean(s));
          objectNode1.put("URL", "https://en.wikipedia.org/wiki/"+cleaner.getTitle(s).replace(" ", "_"));
          
          try {
        	if(!(cleaner.clean(s).contains("#REDIRECT")))
        	{
        		fw.write("{\"index\": {\"_id\": "+cleaner.getId(s)+"}}" + "\n");
        		fw.write(objectNode1.toString() + "\n");
        	}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
          
          //System.out.println(cleaner.getTitle(s));
          //System.out.println(cleaner.clean(s));
//          HttpEntity entity = null;
//		try {
//			entity = new NStringEntity(
//						mapper.writeValueAsString(objectNode1), ContentType.APPLICATION_JSON);
//		} catch (JsonProcessingException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//          Response indexResponse;  
//		try {
//			indexResponse = restClient.performRequest(
//					"PUT",
//					"/wiki/articles/"+cleaner.getId(s),
//					Collections.<String, String>emptyMap(),
//					entity);
//			System.out.println("File " +cleaner.getTitle(s)+" indexed");
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
          cnt.incrementAndGet();
          System.out.println("Copied file "+cnt);
          //System.out.println("Count "+cnt);
          
        });
    
    fw.flush();
    fw.close();

    out.println("Total of " + cnt + " articles read.");
    System.out.println("Indexing " + cnt + "documents took " +(System.currentTimeMillis() - start));
    out.close();
  }
  
  public void createJsonfile() throws IOException
  {
	  WikiLanguage lang = WikiLanguage.EN;

	    PrintStream out = new PrintStream(System.out, true, "UTF-8");
	    WikiClean cleaner = new WikiClean.Builder().withLanguage(lang).build();

	    //WikipediaArticlesDump wikipedia = new WikipediaArticlesDump(args.input);
	    //RestClient restClient = RestClient.builder(new HttpHost("131.234.28.254", 9200, "http")).build();
	    AtomicInteger cnt = new AtomicInteger();
	    FileWriter fw = new FileWriter(new File(this.outputFile));
	    this.stream()
	        // See https://en.wikipedia.org/wiki/Wikipedia:Namespace
	        .filter(s -> !s.contains("<ns>") || s.contains("<ns>0</ns>"))
	        .forEach(s -> {
	          //out.println("Title = " + cleaner.getTitle(s));
	          //out.println("Id = " + cleaner.getId(s));
	          //out.println(cleaner.clean(s) + "\n\n#################################\n");
	          ObjectMapper mapper = new ObjectMapper();
	  			ObjectNode objectNode1 = mapper.createObjectNode();
	          objectNode1.put("Title", cleaner.getTitle(s));
	          objectNode1.put("Article", cleaner.clean(s));
	          objectNode1.put("URL", "https://en.wikipedia.org/wiki/"+cleaner.getTitle(s).replace(" ", "_"));
	          
	          try {
	        	if(!(cleaner.clean(s).contains("#REDIRECT")))
	        	{
	        		fw.write("{\"index\": {\"_id\": "+cleaner.getId(s)+"}}" + "\n");
	        		fw.write(objectNode1.toString() + "\n");
	        	}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	          cnt.incrementAndGet();
	          System.out.println("Copied file "+cnt);
	          //System.out.println("Count "+cnt);
	          
	        });
	    
	    fw.flush();
	    fw.close();
	    out.close();
	  }
  }
