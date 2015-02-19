import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PairCountReducer extends
		Reducer<TextPair, IntWritable, Text, IntWritable> {

	private static Text outputkey = new Text();

	@Override
	public void reduce(TextPair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable val : values) {
			count += val.get();
		}

		outputkey.set(key.toString());
		context.write(outputkey, new IntWritable(count));
	}
}
