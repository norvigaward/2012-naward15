package cwi.commoncrawl;


import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import cwi.arcUtils.ArcInputFormat;
import cwi.arcUtils.ArcRecord;


public class ExtractHtmlFromArc extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(ExtractHtmlFromArc.class);
  private static final String ARGNAME_INPATH = "-in";
  private static final String ARGNAME_OUTPATH = "-out";
  private static final String ARGNAME_CONF = "-conf";
  private static final String ARGNAME_OVERWRITE = "-overwrite";
  private static final String ARGNAME_MAXFILES = "-maxfiles";
  private static final String ARGNAME_NUMREDUCE = "-numreducers";
  private static final String FILEFILTER = ".arc.gz";
  
  protected static enum MAPPERCOUNTER {
    NOT_RECOGNIZED_AS_HTML,
    HTML_PARSE_FAILURE,
    HTML_PAGE_TOO_LARGE,
    EXCEPTIONS,
    OUT_OF_MEMORY
  }

 
  public static class ExampleArcMicroformatMapper extends Mapper<Text, ArcRecord, Text, LongWritable> {
    
    private Document doc;
    private Elements mf;
    private LongWritable outVal = new LongWritable(1);
 
    public void map(Text key, ArcRecord value, Context context) throws IOException {

      try {

        if (!value.getContentType().contains("html")) {
          context.getCounter(MAPPERCOUNTER.NOT_RECOGNIZED_AS_HTML).increment(1);
          return;
        }

        // just curious how many of each content type we've seen
        // TODO: How can we handle this in the new API?
        //reporter.incrCounter(this._counterGroup, "Content Type - "+value.getContentType(), 1);

        // ensure sample instances have enough memory to parse HTML
        if (value.getContentLength() > (5 * 1024 * 1024)) {
          context.getCounter(MAPPERCOUNTER.HTML_PAGE_TOO_LARGE).increment(1);
          return;
        }

        // Count all 'itemtype' attributes referencing 'schema.org'
        doc = value.getParsedHTML();

        if (doc == null) {
          context.getCounter(MAPPERCOUNTER.HTML_PARSE_FAILURE).increment(1);
          return;
        }

        mf = doc.select("[itemtype~=schema.org]");

        if (mf.size() > 0) {
          for (Element e : mf) {
            if (e.hasAttr("itemtype")) {
              context.write(new Text(e.attr("itemtype").toLowerCase().trim()), outVal);
            }
          }
        }
      }
      catch (Throwable e) {

        // occassionally Jsoup parser runs out of memory ...
        if (e.getClass().equals(OutOfMemoryError.class)) {
          context.getCounter(MAPPERCOUNTER.OUT_OF_MEMORY).increment(1);
          System.gc();
        }

        LOG.error("Caught Exception", e);
        context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
      }
    }
  }
  
  public void usage() {
    System.out.println("\n  org.commoncrawl.examples.ExtractHtmlFromArc \n" +
                         "                           " + ARGNAME_INPATH +" <inputpath>\n" +
                         "                           " + ARGNAME_OUTPATH + " <outputpath>\n" +
                         "                         [ " + ARGNAME_OVERWRITE + " ]\n" +
                         "                         [ " + ARGNAME_NUMREDUCE + " <number_of_reducers> ]\n" +
                         "                         [ " + ARGNAME_CONF + " <conffile> ]\n" +
                         "                         [ " + ARGNAME_MAXFILES + " <maxfiles> ]");
    System.out.println("");
    GenericOptionsParser.printGenericCommandUsage(System.out);
  }

  /**
   * Implmentation of Tool.run() method, which builds and runs the Hadoop job.
   *
   * @param  args command line parameters, less common Hadoop job parameters stripped
   *              out and interpreted by the Tool class.  
   * @return      0 if the Hadoop job completes successfully, 1 if not. 
   */
  @Override
  public int run(String[] args) throws Exception {

    String inputPath = null;
    String outputPath = null;
    String configFile = null;
    boolean overwrite = false;
    int numReducers = 1;

    // Read the command line arguments. We're not using GenericOptionsParser
    // to prevent having to include commons.cli as a dependency.
    for (int i = 0; i < args.length; i++) {
      try {
        if (args[i].equals(ARGNAME_INPATH)) {
          inputPath = args[++i];
        } else if (args[i].equals(ARGNAME_OUTPATH)) {
          outputPath = args[++i];
        } else if (args[i].equals(ARGNAME_CONF)) {
          configFile = args[++i];
        } else if (args[i].equals(ARGNAME_MAXFILES)) {
          SampleFilter.setMax(Long.parseLong(args[++i]));
        } else if (args[i].equals(ARGNAME_OVERWRITE)) {
          overwrite = true;
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

    // Read in any additional config parameters.
    if (configFile != null) {
      LOG.info("adding config parameters from '"+ configFile + "'");
      this.getConf().addResource(configFile);
    }

    // Create the Hadoop job.
    Configuration conf = getConf();
    Job job = new Job(conf);
    job.setJarByClass(ExtractHtmlFromArc.class);
    job.setNumReduceTasks(numReducers);

    // Scan the provided input path for ARC files.
    LOG.info("setting input path to '"+ inputPath + "'");
    SampleFilter.setFilter(FILEFILTER);
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileInputFormat.setInputPathFilter(job, SampleFilter.class);

    // Delete the output path directory if it already exists and user wants to overwrite it.
    if (overwrite) {
      LOG.info("clearing the output path at '" + outputPath + "'");
      FileSystem fs = FileSystem.get(new URI(outputPath), conf);
      if (fs.exists(new Path(outputPath))) {
        fs.delete(new Path(outputPath), true);
      }
    }
    
    // Set the path where final output 'part' files will be saved.
    LOG.info("setting output path to '" + outputPath + "'");
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    FileOutputFormat.setCompressOutput(job, false);

    // Set which InputFormat class to use.
    job.setInputFormatClass(ArcInputFormat.class);

    // Set which OutputFormat class to use.
    job.setOutputFormatClass(TextOutputFormat.class);

    // Set the output data types.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    // Set which Mapper and Reducer classes to use.
    job.setMapperClass(ExtractHtmlFromArc.ExampleArcMicroformatMapper.class);
    job.setReducerClass(LongSumReducer.class);

    if (job.waitForCompletion(true)) {
      return 0;
    } else {
      return 1;
    }
  }

  /**
   * Main entry point that uses the {@link ToolRunner} class to run the example
   * Hadoop job.
   */
  public static void main(String[] args)
      throws Exception {
    int res = ToolRunner.run(new Configuration(), new ExtractHtmlFromArc(), args);
    System.exit(res);
  }
}

