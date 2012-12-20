package cwi.commoncrawl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import cwi.commoncrawl.PageDomainCount.MAPPERCOUNTER;

public class ArcLookupIndex extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(ArcLookupIndex.class);

	private static final String ARGNAME_INPATH = "-in";
	private static final String ARGNAME_OUTPATH = "-out";
	private static final String ARGNAME_NUMREDUCE = "-numreducers";
	private static final String ARGNAME_FILEFILTER = "-filefilter";

	/**
	 * Mapping class that filter textData pages for given domain
	 */
	public static class ArcLookupIndexMapper extends
			Mapper<Text, Text, Text, Text> {

		private final Text outKey = new Text();
		private final Text outVal = new Text();

		String url;
		URI uri;
		String host;
		String hostDomain;
		private String fileName;
		String filefilter;
		String[] parts;
		String domainID;

		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObj;
		String json;
		JsonObject arcInfo;
		String arcSourceSegmentId;
		String arcFileDate;
		String arcFilePartition;
		String arcFileOffset;

		@SuppressWarnings("unchecked")
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException {
			Configuration conf = context.getConfiguration();
			filefilter = conf.get("FILE-FILTER");
			InputSplit split = context.getInputSplit();

			FileSplit fileSplit = (FileSplit) split;
			fileName = fileSplit.getPath().getName();

			// key is Text
			url = key.toString();
			json = value.toString();
			try {

				// Get the base domain name
				uri = new URI(url);
				// e.g.,en.cwi.nl
				host = uri.getHost();

				if (host != null) {
					// e.g.,cwi.nl
					hostDomain = InternetDomainName.from(host)
							.topPrivateDomain().name();
					parts = hostDomain.split("\\.");
					domainID = parts[parts.length - 1];

					if (filefilter.equals("textData-")) {

						outKey.set(domainID + "\t" + hostDomain + "\t"
								+ fileName);

						context.write(outKey, outVal);

					}

					if (filefilter.equals("metadata-")) {

						// See if the page has a successful HTTP code
						jsonObj = jsonParser.parse(json).getAsJsonObject();

						arcInfo = jsonObj.get("archiveInfo").getAsJsonObject();

						arcSourceSegmentId = arcInfo.get("arcSourceSegmentId")
								.getAsString();
						arcFileDate = arcInfo.get("arcFileDate").getAsString();
						arcFilePartition = arcInfo.get("arcFileParition")
								.getAsString();
						arcFileOffset = arcInfo.get("arcFileOffset")
								.getAsString();
						outKey.set(domainID);
						outVal.set(key.toString() + "\t" + fileName + "\t"
								
								+ arcSourceSegmentId + "/" + arcFileDate + "_"
								+ arcFilePartition + ".arc.gz" + "\t"
								+ arcFileOffset);
						// outKey.set(domainID + hostDomain + fileName +
						// arcInfo.toString());

						context.write(outKey, outVal);

					}

				}

			} catch (Exception ex) {
				LOG.error("Caught Exception", ex);
			}
		}
	}

	public static class ArcLookupIndexReducer extends
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
		System.out.println("\n  cwi.commoncrawl.FilterSitesPerDomain \n"

		+ ARGNAME_INPATH + " <inputpath>\n" + ARGNAME_OUTPATH
				+ " <outputpath>\n" + ARGNAME_NUMREDUCE + " <numreduc>\n"
				+ ARGNAME_FILEFILTER + " <filefilter>\n");
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
		String fileFilter = null;
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
				} else if (args[i].equals(ARGNAME_FILEFILTER)) {
					fileFilter = args[++i];
				}

				else {
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
		conf.set("FILE-FILTER", fileFilter);
		Job job = new Job(conf);
		job.setJarByClass(ArcLookupIndex.class);
		job.setNumReduceTasks(numReducers);

		// Scan the provided input path
		LOG.info("setting input path to '" + inputPath + "'");
		SampleFilter.setFilter(fileFilter);
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

		// Set which Mapper and Reducer classes to use.
		job.setMapperClass(ArcLookupIndex.ArcLookupIndexMapper.class);
		job.setReducerClass(ArcLookupIndex.ArcLookupIndexReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new ArcLookupIndex(),
				args);
		System.exit(res);
	}
}
