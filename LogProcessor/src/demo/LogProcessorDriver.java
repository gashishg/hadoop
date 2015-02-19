package demo;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import demo.LogProcessorMap.LOG_PROCESSOR_COUNTER;

public class LogProcessorDriver extends Configured implements Tool {
	
	private static Logger log = Logger.getLogger(LogProcessorDriver.class);
	
	static {
		log.info("-- Inside LogProcessorDriver---");
	}
	
	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		log.info("---- Main Method -----");
		Configuration conf = new Configuration();
		/*conf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/lib/hadoop/etc/hadoop/hdfs-site.xml"));*/
		int res = ToolRunner.run(conf, new LogProcessorDriver(),
				args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage:  <input_path> <output_path> <num_reduce_tasks>");
			System.exit(-1);
		}
		
		Configuration conf = getConf();
		
		for (Entry<String, String> entry: conf) {
			System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
			//log.info(entry.getKey()+"-------"+entry.getValue());
		}
		
		/*if(true) {
			System.exit(0);
		}*/
		

		/* input parameters */
		String inputPath = args[0];
		String outputPath = args[1];
		int numReduce = Integer.parseInt(args[2]);

		Job job = Job.getInstance(conf, "log-analysis");

		// DistributedCache.addCacheArchive(new
		// URI("/user/thilina/ip2locationdb.tar.gz#ip2locationdb"),
		// job.getConfiguration());

		job.setJarByClass(LogProcessorDriver.class);
		job.setMapperClass(LogProcessorMap.class);
		job.setReducerClass(LogProcessorReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(LogFileInputFormat.class);
		job.setPartitionerClass(IPBasedPartitioner.class);
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setNumReduceTasks(numReduce);
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
		}

		int exitStatus = job.waitForCompletion(true) ? 0 : 1;

		Counters counters = job.getCounters();
		Counter badRecordsCounter = counters
				.findCounter(LOG_PROCESSOR_COUNTER.BAD_RECORDS);
		System.out.println("# of Bad Records :" + badRecordsCounter.getValue());
		return exitStatus;
	}
}