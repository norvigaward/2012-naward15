package cwi.commoncrawl;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.http.HttpException;
import org.apache.log4j.Logger;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import cwi.arcUtils.ArcInputFormat;
import cwi.arcUtils.ArcRecord;

public class ArcLoader extends LoadFunc {

  private RecordReader<Text, ArcRecord> in;
  private TupleFactory mTupleFactory = TupleFactory.getInstance();
  private static final Logger LOG = Logger.getLogger(ArcLoader.class);

  @Override
  public InputFormat<Text, ArcRecord> getInputFormat() throws IOException {
    return new ArcInputFormat();
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      ArcRecord value = null;
      while (value == null) {
        boolean notDone = in.nextKeyValue();
        if (!notDone) {
          return null;
        }
        value = in.getCurrentValue();
        try {
          value.getHttpResponse();
        } catch (HttpException e) {
          LOG.debug(e.getMessage());
          value = null;
        }
      }
      /**
       * This is where you can put other data in. Nasically anything you can find
       * in org.commoncrawl.hadoop.mapred.ArcRecord you can put into the Tuple. 
       */
      Tuple t = mTupleFactory.newTuple(7);
      t.set(0, value.getArchiveDate().toString());
      t.set(1, value.getContentLength());
      t.set(2, value.getContentType());
      try {
        t.set(3, value.getHttpStatusCode());
      } catch(HttpException e) {
        t.set(3, -1);
      }
      t.set(4, value.getIpAddress());
      t.set(5, value.getURL());
      if (value.getContentType().toLowerCase().contains("html")) {
        t.set(6, value.getParsedHTML().toString());
      } else {
        t.set(6, null);
      }
      return t;
    } catch (InterruptedException e) {
      throw new IOException("Error getting input");
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void prepareToRead(RecordReader reader, PigSplit arg1)
      throws IOException {
    in = reader;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    FileInputFormat.setInputPaths(job, location);
  }

}

