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
			// double m = LR;
			// double b = LR;
			// double prevCost = Double.MAX_VALUE;
			// double currCost = 0.0;
			// double precision = 0.000001;

			// int num_iteration = 100;
			// while(num_iteration > 0) {
			// 	//Decrement num iteration
			// 	num_iteration--;

			// 	// Set initial m and b values in configuration
			// 	conf.set("m", Double.toString(m));
			// 	conf.set("b", Double.toString(b));
            //     conf.set("learningRate", Double.toString(LR)); // Pass learning rate to configuration as well

	
			// 	Job job = Job.getInstance(conf, "GradientDescentParams");
			// 	job.setJarByClass(WordCount.class);
			// 	//pass the m and b values to the mapper. Gets from conf.
			// 	job.setMapperClass(GradientMapper.class);
			// 	//reducer sets new m and b val to conf
			// 	job.setReducerClass(GradientReducer.class);

			// 	//Write the new predicted val of m and b
			// 	job.setOutputKeyClass(Text.class);
			// 	job.setOutputValueClass(DoubleWritable.class);

			// 	FileInputFormat.addInputPath(job, new Path(args[0]));
			// 	job.setInputFormatClass(TextInputFormat.class);

			// 	FileOutputFormat.setOutputPath(job, new Path(args[1] + "_" + num_iteration));
			// 	job.setOutputFormatClass(TextOutputFormat.class);

			// 	//Get one final output
			// 	job.setNumReduceTasks(1);

			// 	job.waitForCompletion(true);

			// 	// After completion, read m, b, and cost from SequenceFile
			// 	Path seqFilePath = new Path("m_b_values.seq");
			// 	readParamsFromSequenceFile(seqFilePath, conf);

			// 	// // Update m and b for next iteration
			// 	// Update m and b for next iteration
			// 	m = Double.parseDouble(conf.get("m"));
			// 	b = Double.parseDouble(conf.get("b"));
            //     currCost = Double.parseDouble(conf.get("cost"));

            //      // Adjust learning rate based on cost comparison
            //      if (currCost < prevCost) {
            //         LR *= 1.05; // Increase learning rate
            //     } else {
            //         LR *= 0.5; // Decrease learning rate
            //     }


			// 	//Print out the cost value
			// 	System.out.println("Cost : "  + conf.get("cost"));
			// 	currCost = Double.parseDouble(conf.get("cost"));

			// 	if(Math.abs(currCost - prevCost) < precision) {
			// 		System.out.println("Convergence");
			// 		break;
			// 	}

			// 	prevCost = currCost;

			// 	//How to adjust the m and b val accoring to the cost calculated
			// }

			// System.out.println("Final m : " + m);
			// System.out.println("Final b : " + b);

			//Task 3

			//Initialize variables to 0.1
			double[] params = {0.1, 0.1, 0.1, 0.1, 0.1};
			double currCost = 0.0;
			double prevCost = Double.MAX_VALUE;
			double precision = 0.00001;

			//Max number of iterations.
			int num_iteration = 100;
			while(num_iteration > 0) {
				num_iteration--;
				//Set all 5 variables for the mapper
				for(int j = 0; j < params.length; j++) {
					conf.set("w" + j, Double.toString(params[j]));
				}
				// pass learning rate in conf for the reducer to use.
				conf.set("learningRate", Double.toString(LR));

				Job job = Job.getInstance(conf, "GD_task3");
				job.setJarByClass(WordCount.class);
				job.setMapperClass(GradientMapperTask3.class);
				job.setReducerClass(GradientReducerTask3.class);

				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(DoubleWritable.class);

				FileInputFormat.addInputPath(job, new Path(args[0]));
				job.setInputFormatClass(TextInputFormat.class);

				FileOutputFormat.setOutputPath(job, new Path(args[1] + "" + num_iteration));
				job.setOutputFormatClass(TextOutputFormat.class);

				//Get one final output
				job.setNumReduceTasks(1);
				job.waitForCompletion(true);

				// After completion, read all variables and cost from seq file.
				Path seqFilePath = new Path("/output/m_b_values.seq");
				T3readParamsFromSequenceFile(seqFilePath, conf);

				// Update all variables for next iteration
				for(int j = 0; j < params.length; j++) {
					params[j] = Double.parseDouble(conf.get("w" + j));
				}
				//Update current cost.
                currCost = Double.parseDouble(conf.get("cost"));

                // Adjust learning rate based on cost comparison
                if (currCost < prevCost) {
                    LR *= 1.05; // Increase learning rate
                } else {
                    LR *= 0.1; // Decrease learning rate
                }

				//Print out the cost value after every iteration
				System.out.println("Cost : "  + currCost);

				//Break out early.
				if(Math.abs(currCost - prevCost) < precision) {
					System.out.println("Convergence");
					break;
				}

				//Update prev cost with the current cost
				prevCost = currCost;
			}

			//print out the final values of the parameters.
			StringBuilder finalValString = new StringBuilder("Final Values: ");
			for (int i = 0; i < params.length; i++) {
				finalValString.append("w").append(i).append(": ").append(params[i]);
				if (i < params.length - 1) {
					finalValString.append(", ");
				}
			}

			System.out.println(finalValString.toString());
			
			return 0;

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
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

	//Helper method to read the updated variables calculated in reducer.
	private void T3readParamsFromSequenceFile(Path seqFilePath, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, seqFilePath, conf);
        Text key = new Text();
        DoubleWritable value = new DoubleWritable();

		try {
			while (reader.next(key, value)) {
				String paramKey = key.toString();
				double paramValue = value.get();
	
				if (paramKey.startsWith("w")) {
					// Update configuration with parameter values w0, w1, w2, w3, w4
					conf.set(paramKey, Double.toString(paramValue));
				} else if (paramKey.equals("cost")) {
					// Update configuration for cost
					conf.set("cost", Double.toString(paramValue));
				}
			}
        } finally {
            reader.close();

			fs.delete(seqFilePath, true); 
        }
    }
}
