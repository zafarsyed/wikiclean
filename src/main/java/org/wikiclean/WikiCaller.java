package org.wikiclean;

import java.io.File;
import java.io.IOException;

public class WikiCaller {

	public static void main(String[] args) throws IOException {
		
		File input = new File("E://enwiki-20170801-pages-articles1.xml-p10p30302.bz2");
		WikipediaArticlesDump wikipedia = new WikipediaArticlesDump(input,"E://articles3.json");
		wikipedia.createJsonfile();
	}

}
