import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class PairCount {
	
	private static Logger log = Logger.getLogger(PairCount.class);
	
	static {
		log.info("--- Inside PairCount class ---");
	}
	
	/**
	 * 
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String args[]) throws IOException,
			InterruptedException, ClassNotFoundException {
		if (args.length != 2) {
			System.err.println("Inavlid Command!");
			System.out.println("Usage: pairCount <input type> and <output>");
			System.exit(0);
		}
		
		String inPath = args[0];
		String outPath = args[1];

		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/hdfs-site.xml"));

		Job job = Job.getInstance(conf);

		job.setJarByClass(PairCount.class);
		job.setJobName("Word Count");

		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.setMapperClass(PairCountMapper.class);
		job.setReducerClass(PairCountReducer.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outPath))) {
			fs.delete(new Path(outPath), true);
		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
