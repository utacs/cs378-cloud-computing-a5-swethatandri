package edu.cs.utexas.HadoopEx;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GradientReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private static final double LEARNING_RATE = 0.001;
    private double m = 0.0;
    private double b = 0.0;

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }

        if (key.toString().equals("mGradient")) {
            m -= LEARNING_RATE * sum;
        } else if (key.toString().equals("bGradient")) {
            b -= LEARNING_RATE * sum;
        }

        context.write(new Text("m"), new DoubleWritable(m));
        context.write(new Text("b"), new DoubleWritable(b));
    }
}