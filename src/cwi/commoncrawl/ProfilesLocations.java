package cwi.commoncrawl;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import com.google.common.net.InternetDomainName;

/**
 * This class shws how to use the jsoup object to extract information from profiles HTML content, such as the location of 
 * the profile.
 */
public class ProfilesLocations extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(ProfilesLocations.class);
	private static final String ARGNAME_INPATH = "-in";
	private static final String ARGNAME_OUTPATH = "-out";
	private static final String FILEFILTER = "part-m-";

	protected static enum MAPPERCOUNTER {
		NOT_RECOGNIZED_AS_HTML, HTML_PARSE_FAILURE, HTML_PAGE_TOO_LARGE, EXCEPTIONS, OUT_OF_MEMORY
	}

	/*
	 * Mapper class which extract the HTML content of the social
	 * sites;linkedin.com, viadeo.com, xing.com from the common-crawl corpus ARC
	 * files
	 */

	public static class ProfilesLocationsMapper extends
			Mapper<Text, Text, Text, Text> {

		private String url;

		String json;
		URI uri;
		String host;
		InternetDomainName idn;
		String hostDomain;
		Text outKey = new Text();
		Text outVal = new Text();

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */

		public void map(Text key, Text value, Context context)
				throws IOException {

			try {

				// key represents a URL (value:HTML for the corresponding
				// key(URL)
				url = key.toString();

				// Check if the URL is a profile

				// if (url.contains("/profile/")) {
				uri = new URI(url);
				idn = InternetDomainName.from(uri.getHost());
				hostDomain = idn.topPrivateDomain().name().toLowerCase();
				// create a jsoup document from the HTML content
				Document doc = Jsoup.parse(value.toString());
				// check if the URL is an Linkedin.com profile
				if (hostDomain.equalsIgnoreCase("linkedin.com")) {

					Elements fullName = doc.select("span[class=full-name");
					if (fullName.hasText()) {

						String profileCountry;
						Elements elements1 = doc
								.select("span[class=country-name]");
						if (elements1.size() > 0) {
							profileCountry = elements1.text().toLowerCase();

						} else {
							Elements elements = doc
									.select("span[class=locality]");
							String profileLocation = elements.text()
									.toLowerCase();
							String[] locationParts = profileLocation
									.split(",\\s");
							profileCountry = locationParts[locationParts.length - 1];
						}
						// output profile's URL as key and it's location

						outKey.set(url);
						outVal.set(profileCountry);

						context.write(outKey, outVal);

					}
				}
				// check if the URL is an viadeo.com profile
				if (hostDomain.equalsIgnoreCase("viadeo.com")) {
					if (url.contains("/profile/")) {
						String profileCountry = "";
						Elements elements1 = doc
								.select("span[class=country-name]");
						Elements elements2 = doc
								.select("abbr[class=country-name]");
						if (elements1.size() > 0) {
							profileCountry = elements1.text().toLowerCase();
						} else if (elements2.size() > 0) {
							profileCountry = elements2.text().toLowerCase();
						}

						// output profile's URL as key and it's location

						outKey.set(url);
						outVal.set(profileCountry);

						context.write(outKey, outVal);
					}

				}
				// check if the URL is an Xing.com profile
				if (hostDomain.equalsIgnoreCase("xing.com")) {
					if (url.contains("/profile/")) {

						Elements elements = doc
								.select("li[class=country-name]");
						String profileCountry = elements.text().toLowerCase();

						// output profile's URL as key and it's location

						outKey.set(url);
						outVal.set(profileCountry);

						context.write(outKey, outVal);
					}

				}

				// }
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

	public static class ProfilesLocationsReducer extends
			Reducer<Text, Text, Text, Text> {

		Text outKey = new Text();
		Text outVal = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text val : values) {
				outKey.set(key.toString());
				outVal.set(val.toString());
				context.write(outKey, outVal);

			}

		}

	}

	public void usage() {
		System.out.println("\n  org.commoncrawl.examples.ProfilesLocations \n"
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

		Job job = new Job(conf);

		job.setJarByClass(ProfilesLocations.class);
		job.setNumReduceTasks(1);

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
		job.setInputFormatClass(SequenceFileInputFormat.class);

		// Set which OutputFormat class to use.
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set the output data types.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// setmapper output data types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Set which Mapper and Reducer classes to use.
		job.setMapperClass(ProfilesLocations.ProfilesLocationsMapper.class);
		job.setReducerClass(ProfilesLocations.ProfilesLocationsReducer.class);
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
		int res = ToolRunner.run(new Configuration(), new ProfilesLocations(),
				args);
		System.exit(res);
	}
}
