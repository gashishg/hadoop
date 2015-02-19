import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import org.apache.hadoop.util.GenericOptionsParser;
/**
 * 
 * @author cloudera
 *
 */
public class WordCountInvertedIndexDriver {
	
	/**
	 * The Mapper class
	 * 
	 * @author cloudera
	 *
	 */
	public static class WordCountMapper extends
			Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private final static Text location = new Text();

		public void setup(Context context) throws IOException,
				InterruptedException {

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			location.set(fileName);
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, location);
			}
		}
	}

	/**
	 * The Reducer class
	 * 
	 * @author cloudera
	 *
	 */
	public static class WordCountReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			boolean first = true;
			StringBuilder toReturn = new StringBuilder();
			
			for (Text value : values) {
				if (first) {
					toReturn.append(value);
					first = false;
				} else if (toReturn.indexOf(value.toString()) == -1) {
					toReturn.append(", ");
					toReturn.append(value);
				}
			}
			context.write(key, new Text("    "+toReturn.toString()));

		}
	}// ending reducer class

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// create an instance of Configuration object
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/hdfs-site.xml"));

		// create an instance of FileSystem that holds Filesystem namespace
		FileSystem fs = FileSystem.get(conf);

		// variables to hold path of input file and output directory
		String[] inPath = new String[3];
		String outPath;

		if (args.length != 2) {
			System.err.println("Usage: wordcount <input file> <output dir>");
			System.out.println("Using default file: WordCount.txt");
			inPath[0] = "/user/cloudera/invertedindex/doc1.txt";
			inPath[1] = "/user/cloudera/invertedindex/doc2.txt";
			inPath[2] = "/user/cloudera/invertedindex/doc3.txt";
			outPath = "/user/cloudera/invertedindex/output";
		} else {
			inPath[0] = args[0];
			outPath = args[1];
		}

		// create an instance of job
		Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(WordCountInvertedIndexDriver.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		if (inPath.length > 1 && inPath[0] != null && inPath[1] != null
				&& inPath[2] != null) {
			FileInputFormat.setInputPaths(job, new Path(inPath[0]), new Path(
					inPath[1]), new Path(inPath[2]) );
		} else {
			FileInputFormat.setInputPaths( job, new Path(inPath[0]) );
		}

		if (fs.exists(new Path(outPath))) {
			fs.delete(new Path(outPath), true);
		}
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
