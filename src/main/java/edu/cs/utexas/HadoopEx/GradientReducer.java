package edu.cs.utexas.HadoopEx;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;


public class GradientReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private static final double LEARNING_RATE = 0.001;
    private double m = 2.0;
    private double b = 3.0;
    private double count = 0.0;
    private double mPartial = 0.0;
    private double bPartial = 0.0;
    private double mSum = 0.0;
    private double bSum = 0.0;

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

        // adjusts m and b based on partial deriv
        // unsure if learning rate supposed to be here
        // mPartial and bPartial = 0 in testing.csv
        m -= LEARNING_RATE * mPartial;
        b -= LEARNING_RATE * bPartial;

        //updating the new predicted m and b to config to pass new val to mapper in next iteration
        Configuration conf = context.getConfiguration();
        conf.set("new m", Double.toString(m));
        conf.set("new b", Double.toString(b));

        // write out new predicted m and b
        context.write(new Text("m"), new DoubleWritable(m));
        context.write(new Text("b"), new DoubleWritable(b));
    }
}