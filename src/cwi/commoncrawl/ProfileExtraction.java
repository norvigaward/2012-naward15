package cwi.commoncrawl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.common.net.InternetDomainName;

import cwi.arcUtils.ArcInputFormat;
import cwi.arcUtils.ArcRecord;

public class ProfileExtraction extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(ProfileExtraction.class);
	private static final String ARGNAME_INPATH = "-in";
	private static final String ARGNAME_OUTPATH = "-out";
	private static final String FILEFILTER = ".arc.gz";

	protected static enum MAPPERCOUNTER {
		NOT_RECOGNIZED_AS_HTML, HTML_PARSE_FAILURE, HTML_PAGE_TOO_LARGE, EXCEPTIONS, OUT_OF_MEMORY
	}

	/*
	 * Mapper class which extract the HTML content of the social
	 * sites;linkedin.com, viadeo.com, xing.com from the common-crawl corpus ARC
	 * files
	 */

	public static class ProfileExtractionMapper extends
			Mapper<Text, ArcRecord, Text, Text> {

		private String urlHtml;
		private String url;

		String json;
		URI uri;
		String host;
		InternetDomainName idn;
		String hostDomain;
		Text outKey = new Text();
		Text outVal = new Text();
		List<String> profileSites = new ArrayList<String>();

		protected void setup(Context context) throws IOException {
			// get the name of social sites that we pass in job configuration
			Configuration conf = context.getConfiguration();
			profileSites = Arrays.asList(conf.get("PROFILE-SITES").split(","));

		};

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */

		public void map(Text key, ArcRecord value, Context context)
				throws IOException {

			try {

				if (!value.getContentType().contains("html")) {
					context.getCounter(MAPPERCOUNTER.NOT_RECOGNIZED_AS_HTML)
							.increment(1);
					return;
				}

				// ensure sample instances have enough memory to parse HTML
				if (value.getContentLength() > (5 * 1024 * 1024)) {
					context.getCounter(MAPPERCOUNTER.HTML_PAGE_TOO_LARGE)
							.increment(1);
					return;
				}
				// get the url from the map (value:ARC record)
				url = value.getURL();
				uri = new URI(url);
				idn = InternetDomainName.from(uri.getHost());
				hostDomain = idn.topPrivateDomain().name();
				/*
				 * check if the url host domain is one of the social sites that
				 * we are extracting; linkedin.com, viadeo.com, xing.com
				 */

				if (profileSites.contains(hostDomain)) {
					// extract HTML
					urlHtml = value.getParsedHTML().toString();

					if (urlHtml != null && url != null) {
						outKey.set(url);
						outVal.set(urlHtml);

						context.write(outKey, outVal);
					}
				}

			} catch (Throwable e) {

				if (e.getClass().equals(OutOfMemoryError.class)) {
					context.getCounter(MAPPERCOUNTER.OUT_OF_MEMORY)
							.increment(1);
					System.gc();
				}

				LOG.error("Caught Exception", e);
				context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
			}
		}
	}

	public void usage() {
		System.out.println("\n  org.commoncrawl.examples.ProfileExtraction \n"
				+ ARGNAME_INPATH + "\t" + " <inputpath>\n"

				+ ARGNAME_OUTPATH + "\t" + " <outputpath>\n");
		System.out.println("");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	/**
	 * Implmentation of Tool.run() method, which builds and runs the Hadoop job.
	 * 
	 * @param args
	 *            command line parameters, less common Hadoop job parameters
	 *            stripped out and interpreted by the Tool class.
	 * @return 0 if the Hadoop job completes successfully, 1 if not.
	 */
	@Override
	public int run(String[] args) throws Exception {

		String inputPath = null;
		String outputPath = null;

		// Read the command line arguments. We're not using GenericOptionsParser
		// to prevent having to include commons.cli as a dependency.
		for (int i = 0; i < args.length; i++) {
			try {
				if (args[i].equals(ARGNAME_INPATH)) {
					inputPath = args[++i];
				} else if (args[i].equals(ARGNAME_OUTPATH)) {
					outputPath = args[++i];
				} else {
					LOG.warn("Unsupported argument: " + args[i]);
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				usage();
				throw new IllegalArgumentException();
			}
		}

		if (inputPath == null || outputPath == null) {
			usage();
			throw new IllegalArgumentException();
		}

		// Create the Hadoop job.
		Configuration conf = getConf();
		conf.set("PROFILE-SITES", "linkedin.com,viadeo.com,xing.com");
		Job job = new Job(conf);

		// add the file which contains the urls of sample into distriuted cache
		DistributedCache.addCacheFile(new URI(
				"common-crawl/enMatches/sample/urlslist/part-m-00000"), job
				.getConfiguration());
		// add the file which contains arcs sample
		DistributedCache.addCacheFile(new URI(
				"common-crawl/enMatches/sample/arcslist/part-m-00000"), job
				.getConfiguration());

		job.setJarByClass(ProfileExtraction.class);
		job.setNumReduceTasks(0);

		// Scan the provided input path for ARC files.
		LOG.info("setting input path to '" + inputPath + "'");
		SampleFilter.setFilter(FILEFILTER);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileInputFormat.setInputPathFilter(job, SampleFilter.class);

		// Set the path where final output 'part' files will be saved.
		LOG.info("setting output path to '" + outputPath + "'");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileOutputFormat.setCompressOutput(job, false);

		// Set which InputFormat class to use.
		job.setInputFormatClass(ArcInputFormat.class);

		// Set which OutputFormat class to use.
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// Set the output data types.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set which Mapper and Reducer classes to use.
		job.setMapperClass(ProfileExtraction.ProfileExtractionMapper.class);
		// job.setReducerClass(LongSumReducer.class);

		if (job.waitForCompletion(true)) {
			return 0;
		} else {
			return 1;
		}
	}

	/**
	 * Main entry point that uses the {@link ToolRunner} class to run the
	 * example Hadoop job.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ProfileExtraction(),
				args);
		System.exit(res);
	}
}
