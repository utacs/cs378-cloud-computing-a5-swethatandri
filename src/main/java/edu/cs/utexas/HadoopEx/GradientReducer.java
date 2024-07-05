package edu.cs.utexas.HadoopEx;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GradientReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    public double m;
    public double b;
    private double count = 0.0;
    private double mPartial = 0.0;
    private double bPartial = 0.0;
    private double mSum = 0.0;
    private double bSum = 0.0;
    private double cost = 0.0;
    private double learningRate;

    @Override
    //Method to help set up all variables before starting the reducing process
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        m = Double.parseDouble(conf.get("m")); // Get initial value for m
        b = Double.parseDouble(conf.get("b")); // Get initial value for b
        learningRate = Double.parseDouble(conf.get("learningRate")); // Get learning rate from configuration
    }

    @Override
    //Aggregate m and b val, cost, and count to calculate the gradient descent of those values through iteration.
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;

        // sums up all vals w/ same key
        for (DoubleWritable val : values) {
            sum += val.get();
        }
        
        // set sum to the respective variable
        switch (key.toString()) {
            case "mGradient":
                mSum = sum;
                break;
            case "bGradient":
                bSum = sum;
                break;
            case "cost":
                cost = sum;
                break;
            case "COUNT":
                count = sum;
                break;
        }
    }

    @Override
    // Calculate the slope partial deriv(m) and the y-intercept partial deriv(b) using gradient descent formula.
    public void cleanup(Context context) throws IOException, InterruptedException { 
        // finds partial derivs for both variables
        // *split up bc sum is diff for both
        mPartial = (2 * mSum) / count;
        bPartial = (2 * bSum) / count;

        // Calculate MSE
        cost = cost / count;

        // Update m using gradient descent
        m -= learningRate * mPartial; 
        // weighting b more in respect to m (bias var)
        // Update b using gradient descent
        b -= learningRate * 100 * bPartial;

        // call to create seq file w new params to pass through driver
        writeParamsToSequenceFile(context, m, b, cost);

        // write out new predicted m and b
        context.write(new Text("m"), new DoubleWritable(m));
        context.write(new Text("b"), new DoubleWritable(b));
        context.write(new Text("Cost"), new DoubleWritable(cost));
    }

    /**
     * Method to write the newly calculated parameter values and cost to sequence file to pass it over to the
     * driver. Writes all the parameter vals and cost to sequential file.
     * 
     * @param context context we're writing on
     * @param m value of the m variable
     * @param b value of the b(bias) variable
     * @param cost cost function of the current iteration
     * @throws IOException
     */
    private void writeParamsToSequenceFile(Context context, double m, double b, double cost) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        // Define the path for the SequenceFile
        Path filePath = new Path("m_b_values.seq");

        // Create SequenceFile Writer
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, filePath, Text.class, DoubleWritable.class);

        try {
            // Write m, b, and cost to SequenceFile
            writer.append(new Text("m"), new DoubleWritable(m));
            writer.append(new Text("b"), new DoubleWritable(b));
            writer.append(new Text("cost"), new DoubleWritable(cost));
        } finally {
            // Close the writer
            writer.close();
        }
    }
}