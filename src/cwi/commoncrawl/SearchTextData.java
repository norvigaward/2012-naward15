package cwi.commoncrawl;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.net.InternetDomainName;

public class SearchTextData extends Configured implements
		Tool {

	private static final Logger LOG = Logger
			.getLogger(SearchTextData.class);

	private static final String ARGNAME_INPATH = "-in";
	private static final String ARGNAME_OUTPATH = "-out";
	private static final String ARGNAME_NUMREDUCE = "-numreducers";
	private static final String FILEFILTER = "textData-";
	private static final String PATH_PREFIX = "hdfs://p-head03.alley.sara.nl/data/public/common-crawl/award/testset/";

	private static final String[] keyWords = { "gezocht", "gevraagd",
			"vacature", "vacatures", "vakature", "vakatures", "vacaturenummer",
			"referentienummer", "taakomschrijving", "functieomschrijving",
			"functie omschrijving", "doel van de functie", "sollicitatie",
			"sollicitaties", "solliciteren" };

	/**
	 * Mapping class that filter textData pages for given domain
	 */
	public static class ExtractTextDataPerDomainMapper extends
			Mapper<Text, Text, Text, LongWritable> {

		private final Text outKey = new Text();
		private static final LongWritable outVal = new LongWritable(1);

		String url;
		URI uri;
		String host;
		String hostDomain;
		String givenDomain;
		String pageText;
		String outKeys;
		int outTF;
		private String fileName;
		String[] parts;
		String domainID;
		int keyworgTF = 0;
		HashMap<String, Integer> matchedKeys = new HashMap<String, Integer>();
		String appendMathes;

		@SuppressWarnings("unchecked")
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException {
			Configuration conf = context.getConfiguration();
			// givenDomain = conf.get("GIVEN-DOMAIN");
			InputSplit split = context.getInputSplit();

			FileSplit fileSplit = (FileSplit) split;
			fileName = fileSplit.getPath().getName();
			Set set;
			// key is Text
			url = key.toString();
			pageText = value.toString().toLowerCase();

			try {

				// Get the base domain name
				uri = new URI(url);
				// e.g.,en.cwi.nl
				host = uri.getHost();

				if (host != null) {
					// e.g.,cwi.nl
					hostDomain = InternetDomainName.from(host).topPrivateDomain()
							.name();
					 parts = hostDomain.split("\\.");
					 domainID = parts[parts.length - 1];
					if (domainID.equalsIgnoreCase("nl")) {
						// || domain.equalsIgnoreCase("uk")
						// /|| domain.equalsIgnoreCase("nl")
						// || domain.equalsIgnoreCase("pl")
						// || domain.equalsIgnoreCase("se")
						// || domain.equalsIgnoreCase("dk")
						// || domain.equalsIgnoreCase("no")
						// || domain.equalsIgnoreCase("fi")
						// || domain.equalsIgnoreCase("fr")
						// || domain.equalsIgnoreCase("com")) {
						for (String vacancyKW : keyWords) {
							if (StringUtils.containsIgnoreCase(pageText,
									vacancyKW)) {

								keyworgTF = StringUtils.countMatches(pageText,
										vacancyKW);
								matchedKeys.put(vacancyKW, keyworgTF);

								
							}

						}

						set = matchedKeys.entrySet();
						Iterator iter = set.iterator();

						if (matchedKeys.size() >= 2) {
							while (iter.hasNext()) {

								Map.Entry<String, Integer> e = (Map.Entry<String, Integer>) iter
										.next();
								if (e.getValue() > 0) {
									outKey.set(domainID + "," + hostDomain + ","
											+ fileName + "," + e.getKey() + ","
											+ Integer.toString(e.getValue()));

									context.write(outKey, outVal);
								}

							}

						}

					}

				}

			} catch (Exception ex) {
				LOG.error("Caught Exception", ex);
			}
		}
	}
	
	
	
	public static class ExtractTextDataPerDomainReducer extends
	Reducer <Text, Iterable<LongWritable>, Text, LongWritable> {
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context){
			
			for(LongWritable val : values){
				
				
				
			}
			
		}
		
	}
	

	public void usage() {
		System.out.println("\n  cwi.commoncrawl.ExtractTextDataPerDomain \n"

		+ ARGNAME_INPATH + " <inputpath>\n" + ARGNAME_OUTPATH
				+ " <outputpath>\n" + ARGNAME_NUMREDUCE + " <numreduc>\n");
		System.out.println("");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	/**
	 * Implementation of Tool.run() method, which builds and runs the Hadoop
	 * job.
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
		int numReducers = 1;

		// Read the command line arguments.

		for (int i = 0; i < args.length; i++) {
			try {
				if (args[i].equals(ARGNAME_INPATH)) {
					inputPath = args[++i];
				} else if (args[i].equals(ARGNAME_OUTPATH)) {
					outputPath = args[++i];
				} else if (args[i].equals(ARGNAME_NUMREDUCE)) {
					numReducers = Integer.parseInt(args[++i]);
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
		job.setJarByClass(SearchTextData.class);
		job.setNumReduceTasks(numReducers);

		// Scan the provided input path
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
		job.setOutputValueClass(LongWritable.class);

		// Set which Mapper and Reducer classes to use.
		job.setMapperClass(SearchTextData.ExtractTextDataPerDomainMapper.class);
		job.setReducerClass(LongSumReducer.class);

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
		int res = ToolRunner.run(new Configuration(),
				new SearchTextData(), args);
		System.exit(res);
	}
}
