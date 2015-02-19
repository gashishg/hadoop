package demo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

public class LogFileInputFormat extends
		FileInputFormat<LongWritable, LogWritable> {

	private static Logger log = Logger.getLogger(LogFileInputFormat.class);

	static {
		log.info("--- Inside LogFileInputFormat Ashish----");
	}

	@Override
	public RecordReader<LongWritable, LogWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		return new LogFileRecordReader();
	}

}
