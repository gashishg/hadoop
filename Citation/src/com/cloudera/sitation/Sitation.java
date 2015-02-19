/**
 * 
 */
package com.cloudera.sitation;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * @author cloudera
 *
 */
public class Sitation {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		// holds the the configurations
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/hdfs-site.xml"));

		// CREATE a Job object
		Job job = Job.getInstance(conf, "Sitation");
		String inPath;
		String outPath;

		// Local Mode
		inPath = "/user/cloudera/sitation/cite75_99.txt";
		outPath = "/user/cloudera/sitation/output";

		// Pseudo Mode
		// inPath = args[0];
		// outPath = args[1];

		job.setJobName("Sitation Run");
		job.setJarByClass(Sitation.class);
		job.setMapperClass(SitationMapper.class);
		job.setCombinerClass(SitationReducer.class);
		job.setReducerClass(SitationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(5);
		FileInputFormat.setInputPaths(job, new Path(inPath));
		// FileInputFormat.addInputPath(job, new Path(inPath));
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
}

// Mapper<dataype of input key , datatype of input value , data type of
// intermediate key , datatype of intermediat value>
/**
 * 
 * @author cloudera
 *
 */
class SitationMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static Logger log = Logger.getLogger(SitationMapper.class);

	static {
		log.info("---Inside SitationMapper, the Mapped Class -----");
	}

	static enum Counters {
		bad, good
	};

	private Text wValue = new Text();
	private Text word = new Text();
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		log.info("--- Inside SitationMapper class and calling setUp() Method ------");
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		/*log.info("Key : " + key.get() + "   Value:"
				+ value.toString());*/

		StringTokenizer token = new StringTokenizer(value.toString(), ",");

		/*
		 * while (token.hasMoreTokens()) { word.set(token.nextToken());
		 * System.out.println("Word : " + word.toString());
		 * 
		 * wValue.set(token.nextToken()); System.out.println("wValue : " +
		 * wValue.toString()); // if(!(word.toString().equals("car") || //
		 * word.toString().equals("jeep") || word.toString().equals("suv")))
		 * context.write(word, wValue); }
		 */
		if (token.countTokens() == 2) {
			word.set(token.nextToken());
			//log.info("Word : " + word.toString());

			wValue.set(token.nextToken());
			//log.info("wValue : " + wValue.toString());

			context.write(word, wValue);
		} else {
			context.getCounter(Counters.bad).increment(1);
		}

		context.getCounter(Counters.good).increment(1);
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		log.info("--- Inside SitationMapper class and calling CLEANUP() Method ------");
	}
}

/**
 * 
 * @author cloudera
 *
 */
class SitationReducer extends Reducer<Text, Text, Text, Text> {
	
	private static Logger log = Logger.getLogger(SitationReducer.class);
	
	static {
		log.info("---Inside SitationReducer, the Reducer Class -----");
	}
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		log.info("--- Inside SitationReducer class and calling setUp() Method ------");
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		/*
		 * log.info("Reducer Key : " + key.toString() +
		 * "    Values : " + values.toString());
		 */
		StringBuilder finalText = new StringBuilder();
		for (Text value : values) {
			finalText.append(value.toString());
			finalText.append(",");
		}
		System.out.println("Reducer Key : " + key.toString() + "    Values : "
				+ finalText.toString());
		context.write(
				key,
				new Text(finalText.toString().substring(0,
						finalText.toString().lastIndexOf(","))));
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		log.info("--- Inside SitationReducer class and calling CLEANUP() Method ------");
	}
}
