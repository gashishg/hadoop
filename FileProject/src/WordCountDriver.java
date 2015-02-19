import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

	/**
	 * @param args
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		// holds the the configurations
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/hdfs-site.xml"));

		// CREATE a Job object
		Job job = new Job(conf, "Word Count");
		String inPath;
		String outPath;

		// Local Mode
		inPath = "/user/cloudera/firstDir/test.txt";
		outPath = "/user/cloudera/wordcountoutput";

		// Pseudo Mode
		// inPath = args[0];
		// outPath = args[1];

		job.setJobName("Wordcount Run");
		job.setJarByClass(WordCountDriver.class);
		job.setMapperClass(WordCountMapper.class);
		// job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outPath))) {
			fs.delete(new Path(outPath), true);
		}
		try {
			job.waitForCompletion(true);
			System.out.println("Job Finished");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Mapper<dataype of input key , datatype of input value , data type of
	// intermediate key , datatype of intermediat value>

	public static class WordCountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			System.out.println("Key : " + key.get() + "   Value:"
					+ value.toString());

			StringTokenizer token = new StringTokenizer(value.toString());

			while (token.hasMoreTokens()) {
				word.set(token.nextToken());
				System.out.println("Word : " + word.toString());

				// if(!(word.toString().equals("car") ||
				// word.toString().equals("jeep") ||
				// word.toString().equals("suv")))
				context.write(word, one);
			}
		}
	}

	public static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			System.out.println("Reducer Key : " + key.toString()
					+ "    Values : " + values.toString());
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}
