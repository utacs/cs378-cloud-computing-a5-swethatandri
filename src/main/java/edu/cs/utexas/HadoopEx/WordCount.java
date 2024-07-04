package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
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

    public static double LR = 0.001;

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
			double prevCost = Double.MAX_VALUE;
			double currCost = 0.0;
			double precision = 0.000001;

			int num_iteration = 100;
			while(num_iteration > 0) {
				//Decrement num iteration
				num_iteration--;

				// Set initial m and b values in configuration
				conf.set("m", Double.toString(m));
				conf.set("b", Double.toString(b));
                conf.set("learningRate", Double.toString(LR)); // Pass learning rate to configuration as well

	
				Job job = Job.getInstance(conf, "GradientDescentParams");
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

				FileOutputFormat.setOutputPath(job, new Path(args[1] + "_" + num_iteration));
				job.setOutputFormatClass(TextOutputFormat.class);

				//Get one final output
				job.setNumReduceTasks(1);

				job.waitForCompletion(true);

				// After completion, read m, b, and cost from SequenceFile
				Path seqFilePath = new Path("/output/m_b_values.seq");
				readParamsFromSequenceFile(seqFilePath, conf);

				// // Update m and b for next iteration
				// Update m and b for next iteration
				m = Double.parseDouble(conf.get("m"));
				b = Double.parseDouble(conf.get("b"));
                currCost = Double.parseDouble(conf.get("cost"));

                 // Adjust learning rate based on cost comparison
                 if (currCost < prevCost) {
                    LR *= 1.05; // Increase learning rate
                } else {
                    LR *= 0.5; // Decrease learning rate
                }


				//Print out the cost value
				System.out.println("Cost : "  + conf.get("cost"));
				currCost = Double.parseDouble(conf.get("cost"));

				if(Math.abs(currCost) < precision) {
					System.out.println("Convergence");
					break;
				}

				prevCost = currCost;

				//How to adjust the m and b val accoring to the cost calculated
			}

			System.out.println("Final m : " + m);
			System.out.println("Final b : " + b);
			return 0;

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
		// Task 3 Brainstorming??

		 //Task 3

        /*
         * 	double w0 = 0.0;
			double w1 = 0.0;
			double w2 = 0.0;
			double w3 = 0.0;
			double w4 = 0.0;
			double learningRate = 0.001;
			int NUM_ITERATIONS = 100;

			for (int i = 0; i < NUM_ITERATIONS; i++) {
				Configuration iterationConf = new Configuration();
				iterationConf.set("w0", Double.toString(w0));
				iterationConf.set("w1", Double.toString(w1));
				iterationConf.set("w2", Double.toString(w2));
				iterationConf.set("w3", Double.toString(w3));
				iterationConf.set("w4", Double.toString(w4));
				iterationConf.set("learningRate", Double.toString(learningRate));

				Job job = Job.getInstance(iterationConf, "Gradient Descent Task 3 - Iteration " + (i + 1));
				job.setJarByClass(WordCount.class);
				job.setMapperClass(GradientMapperTask3.class);
				job.setReducerClass(GradientReducerTask3.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(DoubleWritable.class);

				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1] + "_iteration_" + (i + 1)));

				job.waitForCompletion(true);

				// Retrieve updated parameters from job output
				Path outputPath = new Path(args[1] + "_iteration_" + (i + 1) + "/part-r-00000");
				FileSystem fs = FileSystem.get(conf);
				FSDataInputStream inputStream = fs.open(outputPath);
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
				String line;
				Map<String, Double> parameters = new HashMap<>();
				while ((line = reader.readLine()) != null) {
					String[] parts = line.split("\t");
					parameters.put(parts[0], Double.parseDouble(parts[1]));
				}
				reader.close();

				w0 = parameters.get("w0");
				w1 = parameters.get("w1");
				w2 = parameters.get("w2");
				w3 = parameters.get("w3");
				w4 = parameters.get("w4");

				System.out.println("Iteration " + (i + 1) + ": w0 = " + w0 + ", w1 = " + w1 + ", w2 = " + w2 + ", w3 = " + w3 + ", w4 = " + w4);
			}

			return 0;

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
         */
	}

	private void readParamsFromSequenceFile(Path seqFilePath, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, seqFilePath, conf);
        Text key = new Text();
        DoubleWritable value = new DoubleWritable();

        try {
            while (reader.next(key, value)) {
                System.out.println(key.toString() + " = " + value.get());
                //key is m, b, and cost
                if (key.toString().equals("m")) {
                    conf.set("m", Double.toString(value.get()));
                } else if (key.toString().equals("b")) {
                    conf.set("b", Double.toString(value.get()));
                } else if (key.toString().equals("cost")) {
					conf.set("cost", Double.toString(value.get()));
				}
            }
        } finally {
            reader.close();

			fs.delete(seqFilePath, true); 
        }
    }
}