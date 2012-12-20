package cwi.commoncrawl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class CreateSample extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(CreateSample.class);

	private static final String ARGNAME_INPATH = "-in";
	private static final String ARGNAME_OUTPATH = "-out";
	private static final String ARGNAME_NUMREDUCE = "-numreducers";

	/**
	 * Mapping class that filter textData pages for given domain
	 */
	public static class CreateSampleMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		Text outKey = new Text();
		Text outVal = new Text();
		String domainID;
		String url;
		String[] inputLine;

		@SuppressWarnings("unchecked")
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			inputLine = value.toString().split("\t");

			outKey.set(inputLine[0]);
			outVal.set(inputLine[1]);
			context.write(outKey, outVal);
			// String output = outKey + "\t" + outVal;
			// LOG.info(output);

		}

	}

	public static class CreateSampleReducer extends
			Reducer<Text, Text, Text, Text> {

		Text outKey = new Text();
		Text outVal = new Text();

		private Path[] statFiles;
		HashMap<String, Integer> temp = new HashMap<String, Integer>();
		int totalURLsNum;
		int sample;
		int domainURLsNum;

		protected void setup(Context context) throws IOException {

			BufferedReader br;
			Configuration conf = context.getConfiguration();
			totalURLsNum = Integer.parseInt(conf.get("totalURLsNum"));
			sample = Integer.parseInt(conf.get("sample"));
			statFiles = DistributedCache.getLocalCacheFiles(conf);

			for (Path statFile : statFiles) {
				try {
					// in = fs.open(localFile);
					br = new BufferedReader(new FileReader(statFile.toString()));
					String line = "";
					String[] lineComp;

					while ((line = br.readLine()) != null) {
						lineComp = line.split("\t");
						temp.put(lineComp[0], Integer.parseInt(lineComp[1]));

						// LOG.info(line);
					}

				} catch (FileNotFoundException e1) {
					e1.printStackTrace();
					System.out
							.println("read from distributed cache: file not found!");
				} catch (IOException e1) {
					e1.printStackTrace();
					System.out
							.println("read from distributed cache: IO exception!");
				}

			}

		};

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// ArrayList<Text> urlsList = new ArrayList<Text>();
			// ArrayList<Text> sampleList = new ArrayList<Text>();
			domainURLsNum = temp.get(key.toString());
			int m = Math.round(((float) sample / (float) totalURLsNum)
					* (float) domainURLsNum);
			int count = 0;
			
			outKey.set(key.toString());

			for (Text val : values) {
				// urlsList.add(val);
				if (count < m) {
					outVal.set(val.toString());
					context.write(outKey, outVal);
					
				}
				count++;

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

		conf.set("totalURLsNum", "278446016");
		conf.set("sample", "100000");
		conf.setLong("mapred.task.timeout", 360000000);
		conf.set("mapred.map.child.java.opts",
				"-Xmx3000m -XX:-UseGCOverheadLimit");
		conf.set("mapred.reduce.child.java.opts",
				"-Xmx5000m -XX:-UseGCOverheadLimit");
		Job job = new Job(conf);
		DistributedCache
				.addCacheFile(
						new URI(
								"hdfs://p-head03.alley.sara.nl/user/tsamar/common-crawl/filters/DomainVacancyCount_en"),
						job.getConfiguration());

		job.setJarByClass(CreateSample.class);
		job.setNumReduceTasks(numReducers);

		// Scan the provided input path
		LOG.info("setting input path to '" + inputPath + "'");

		FileInputFormat.addInputPath(job, new Path(inputPath));

		// Set the path where final output 'part' files will be saved.
		LOG.info("setting output path to '" + outputPath + "'");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileOutputFormat.setCompressOutput(job, false);

		// Set which InputFormat class to use.
		job.setInputFormatClass(TextInputFormat.class);

		// Set which OutputFormat class to use.
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set the output data types.

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Set which Mapper and Reducer classes to use.
		job.setMapperClass(CreateSample.CreateSampleMapper.class);
		// job.setCombinerClass(CreateSample.CreateSampleReducer.class);
		job.setReducerClass(CreateSample.CreateSampleReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new CreateSample(), args);
		System.exit(res);
	}
}