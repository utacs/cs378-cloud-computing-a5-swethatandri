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
    private double LEARNING_RATE = 0.01;
    public double m = 2.0;
    public double b = 3.0;
    private double count = 0.0;
    private double mPartial = 0.0;
    private double bPartial = 0.0;
    private double mSum = 0.0;
    private double bSum = 0.0;
    private double cost = 0.0;

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;

        // sums up all vals w/ same key
        for (DoubleWritable val : values) {
            sum += val.get();

            // use to check what values pushing through
            // context.write (new Text("VAL GOTTEN: "), new DoubleWritable(val.get()));
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
        // use to check count (N)
        // context.write (new Text("count"), new DoubleWritable(count));
    }

    @Override
    // Calculate the slope partial deriv(m) and the y-intercept partial deriv(b) using gradient descent formula.
    public void cleanup(Context context) throws IOException, InterruptedException { 
        // finds partial derivs for both variables
        // *split up bc sum is diff for both
        mPartial = (2 * mSum) / count;
        bPartial = (2 * bSum) / count;

        // Calculate MSE
        // ed discussion 1/N factor?
        cost = cost / count;

        //Adjusting the learning rate depending on cost?
        if(cost < 1) {
            LEARNING_RATE = 0.01;
        } else {
            LEARNING_RATE = 0.1;
        }
        // adjusts m and b based on partial deriv
        // unsure if learning rate supposed to be here
        // mPartial and bPartial = 0 in testing.csv
        m -= LEARNING_RATE * mPartial;
        b -= LEARNING_RATE * bPartial;


        //updating the new predicted m and b to config to pass new val to mapper in next iteration
        // Configuration conf = context.getConfiguration();
        // conf.set("m", Double.toString(m));
        // conf.set("b", Double.toString(b));
        writeParamsToSequenceFile(context, m, b, cost);
        // write out new predicted m and b
        context.write(new Text("m"), new DoubleWritable(m));
        context.write(new Text("b"), new DoubleWritable(b));
        context.write(new Text("Cost"), new DoubleWritable(cost));
        
    }

    private void writeParamsToSequenceFile(Context context, double m, double b, double cost) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        // Define the path for the SequenceFile
        Path filePath = new Path("/output/m_b_values.seq");

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