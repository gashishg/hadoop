import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FirstFile {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.addResource(new Path(
					"/usr/lib/hadoop/etc/hadoop/core-site.xml"));
			conf.addResource(new Path(
					"/usr/lib/hadoop/etc/hadoop/hdfs-site.xml"));

			FileSystem fs = FileSystem.get(conf);

			Path inFile = new Path(
					"/user/cloudera/firstDir/file1");

			// Checks
			if (!fs.exists(inFile)) {
				System.out.println("Input file not found");
				System.exit(0);
			}
			if (!fs.isFile(inFile)) {
				System.out.println("Input should be a file");
				System.exit(0);
			}

			FSDataInputStream in = fs.open(inFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;
			line = br.readLine();
			while (line != null) {
				System.out.println(line);
				line = br.readLine();
			}
			in.close();
			fs.close();
		} catch (Exception e) {
			System.out.println("Got Exception : " + e);
		}
	}

}
