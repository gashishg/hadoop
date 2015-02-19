import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteHdfs {
	public static void main(String[] args) {
		try {
			FSDataOutputStream out;
			// You should have a conf object and resource addded to it.
			Configuration conf = new Configuration();
			conf.addResource(new Path(
					"/usr/lib/hadoop/etc/hadoop/core-site.xml"));
			conf.addResource(new Path(
					"/usr/lib/hadoop/etc/hadoop/hdfs-site.xml"));
			
			//StringBuffer buf = new StringBuffer("");

			// FileSystem object
			FileSystem fs = FileSystem.get(conf);

			Path outFile = new Path(
					"/user/cloudera/training/input/writeHdfs.txt");

			// Checks
			if (fs.exists(outFile)) {
				System.out.println("Output File Already exist");
				fs.delete(outFile);
				//System.exit(0);
			} //else {
				out = fs.create(outFile);
				StringBuilder input = new StringBuilder("New File Created Today");
				input.append("\n");
				input.append("This is file1 - Test");
				input.append("\n");
				out.write(input.toString().getBytes());
				out.close();
				fs.close();
				System.out.println("End of Program");
			//}
		} catch (Exception e) {
			System.out.println("Got Exception : " + e);
		}
	}
}