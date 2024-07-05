package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GradientReducerTask3 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private double learningRate;
    //total number of data to process
    private double totalCount = 0.0;
    //Hold values for all paramters(w0,w1,w2,w3,w4)
    private double[] params;
    //Hold the partial derivative values for all params.
    private double[] gradientSums;
    private double[] partialDerivs;
    private double cost = 0.0;

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();

        //get current values for all parameters.
        params = new double[5];
        for(int i = 0; i < params.length; i++) {
            params[i] = Double.parseDouble(conf.get("w" + i));
        }

        //array to hold the accumulated sums of y - pred y for each param
        gradientSums = new double[5];
        //values after the summation (-1 (gradientSum) / totalCount)
        partialDerivs = new double[5];

        //Get learning rate from configuration.
        learningRate = Double.parseDouble(conf.get("learningRate"));
        System.out.println("Learning rate: " + learningRate);
        
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;

        //sums up all vals with the same key
        for (DoubleWritable val : values) {
            sum += val.get();
        }

        //TO DO: ADD the COST variable in mapper and reducer too...
        
        //Set sum to each of the corresponding variables.
        if(key.toString().equals("Count")) {
            totalCount += sum;
        } else if(key.toString().equals("Cost")) {
            cost = sum;
        } else {
            //extract the ith val of the parameters: getting the 1 from w1, and so on.
            int index = Integer.parseInt(key.toString().substring(1));
            //Get the gradient sums for all parameters.
            gradientSums[index] = sum;
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        //Find the partial deriv for all parameters. gradientsums length = 5
        for(int i = 0; i < gradientSums.length; i++) {
            //Includes partial deriv for bias variable.
            partialDerivs[i] = (-1 * gradientSums[i]) / totalCount;
        }

        // Calculate MSE: summation of error^2 / 2N
        cost = cost / (2 * totalCount);

        // updating bias variable, adjusting it differently similar to how we did in task 2.
        params[0] -= learningRate * 1000 * partialDerivs[0];
        // update the rest of the params. param length = 5.
        for(int i = 1; i < params.length; i++) {
            //params 1~4
            params[i] -= learningRate * 10 * partialDerivs[i];
        }

        //Write the params to the sequence file to get the val and update it in driver.
        writeParamsToSequenceFile(context, params, cost);

        //Write the pred values and cost to context.
        for(int i = 0; i < params.length; i++) {
            context.write(new Text("w" + i), new DoubleWritable(params[i]));
        }
        
        context.write(new Text("Cost"), new DoubleWritable(cost));
    }

    //Helper method to write the reduced vals: updated params and cost to a sequence file.
    private void writeParamsToSequenceFile(Context context, double[] param, double cost) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        // Define the path for the SequenceFile
        Path filePath = new Path("/output/m_b_values.seq");

        // Create SequenceFile Writer
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, filePath, Text.class, DoubleWritable.class);

        try {
            // Update the parameter values for next iteration by writing to seq file.
            for(int i = 0; i < param.length; i++) {
                writer.append(new Text("w" + i), new DoubleWritable(param[i]));
            }
            //Write the cost.
            writer.append(new Text("cost"), new DoubleWritable(cost));
        } finally {
            // Close the writer
            writer.close();
        }
    }
}

