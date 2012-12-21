package cwi.commoncrawl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InternetDomainName;

public class Test {
	private static final String PATH_PREFIX = "hdfs://p-head03.alley.sara.nl";

	public static void main(String[] args) throws IOException, URISyntaxException {

		

		

		String path = "hdfs://p-head03.alley.sara.nl/data/public/common-crawl/parse-output/segment/1346876860493/1346905341564_2420.arc.gz";
		System.out.println(path);
		System.out.println(PATH_PREFIX);
		System.out.println(StringUtils.remove(path, PATH_PREFIX));
		System.out.println();

		

		String url = "http://www.pixelache.ac/helsinki/2010/greetings-from-kultivator-dyestad-farm-oland-sweden/";
		URI uri =new URI(url);
		InternetDomainName domainName = InternetDomainName.from(uri.getHost());
		String domain = domainName.topPrivateDomain().name();
System.out.println(domainName.toString() + "\t"+domain);
		
		

		
		
	}

}
