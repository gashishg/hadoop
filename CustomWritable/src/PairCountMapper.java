import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class PairCountMapper extends
		Mapper<LongWritable, Text, TextPair, IntWritable> {
	
	private static Logger log = Logger.getLogger(PairCountMapper.class);
	
	static {
		log.info("--- Inside PairCountMapper class ---");
	}

	private static Text firstWord = null;
	private static TextPair textPair = new TextPair();
	private static Text nextWord = new Text();
	private static IntWritable one = new IntWritable(1);

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		line = line.replace(",", ""); // remove comma from the text
		line = line.replace(".", ""); // remove fullstop from the text

		for (String word : line.split("\\W+")) {
			if (firstWord == null) // check if the incoming word is first word
			{
				firstWord = new Text(word);
			} else {
				nextWord.set(word); // set the second word for the pair
				textPair.set(firstWord, nextWord); // set both words in textPair
				context.write(textPair, one); // write word on the output
				firstWord.set(nextWord.toString()); // make second word as first
													// word for the next pair
			}
		}
	}
}
