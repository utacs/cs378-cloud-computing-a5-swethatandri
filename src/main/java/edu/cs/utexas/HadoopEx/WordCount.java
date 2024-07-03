package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(res);
	}

	/**
	 * 
	 */
	public int run(String args[]) {
		try {
			Configuration conf = new Configuration();

			//Task 1
			// Job job = new Job(conf, "LinearRegression");
			// job.setJarByClass(WordCount.class);
			// job.setMapperClass(WordCountMapper.class);
			// job.setReducerClass(WordCountReducer.class);
			// job.setOutputKeyClass(Text.class);
			// job.setOutputValueClass(FloatWritable.class);
			// FileInputFormat.addInputPath(job, new Path(args[0]));
			// job.setInputFormatClass(TextInputFormat.class);
			// FileOutputFormat.setOutputPath(job, new Path(args[1]));
			// job.setOutputFormatClass(TextOutputFormat.class);
			// job.setNumReduceTasks(1);
			// return (job.waitForCompletion(true) ? 0 : 1);

			//Task 2
			//initialize m and b = learning rate
			double m = 0.001;
			double b = 0.001;
			//Set the m and b variables before mapping
			conf.set("m", Double.toString(m));
			conf.set("b", Double.toString(b));

			int num_iteration = 100;
			for(int i = 0; i < num_iteration; i++) {
				Job job = new Job(conf, "GradientDescentParams");
				job.setJarByClass(WordCount.class);
				//pass the m and b values to the mapper. Gets from conf.
				job.setMapperClass(GradientMapper.class);
				//reducer sets new m and b val to conf
				job.setReducerClass(GradientReducer.class);

				//Write the new predicted val of m and b
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(DoubleWritable.class);

				FileInputFormat.addInputPath(job, new Path(args[0]));
				job.setInputFormatClass(TextInputFormat.class);
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				job.setOutputFormatClass(TextOutputFormat.class);

				//Get one final output
				job.setNumReduceTasks(1);

				//Job is completed.
				if(!job.waitForCompletion(true)) {
					return 1;
				}

				//update the m and b val with the new predicted values
				m = Double.parseDouble(job.getConfiguration().get("new m"));
				b = Double.parseDouble(job.getConfiguration().get("new b"));

				conf.set("m", Double.toString(m));
				conf.set("b", Double.toString(b));
		}

		return 0;

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
	}
}
