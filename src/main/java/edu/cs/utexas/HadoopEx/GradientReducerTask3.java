package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GradientReducerTask3 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private double w0;
    private double w1;
    private double w2;
    private double w3;
    private double w4;
    private double learningRate;

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        w0 = Double.parseDouble(conf.get("w0"));
        w1 = Double.parseDouble(conf.get("w1"));
        w2 = Double.parseDouble(conf.get("w2"));
        w3 = Double.parseDouble(conf.get("w3"));
        w4 = Double.parseDouble(conf.get("w4"));
        learningRate = Double.parseDouble(conf.get("learningRate"));
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }

        switch (key.toString()) {
            case "w0Gradient":
                w0 -= learningRate * sum;
                break;
            case "w1Gradient":
                w1 -= learningRate * sum;
                break;
            case "w2Gradient":
                w2 -= learningRate * sum;
                break;
            case "w3Gradient":
                w3 -= learningRate * sum;
                break;
            case "w4Gradient":
                w4 -= learningRate * sum;
                break;
        }

        context.write(new Text("w0"), new DoubleWritable(w0));
        context.write(new Text("w1"), new DoubleWritable(w1));
        context.write(new Text("w2"), new DoubleWritable(w2));
        context.write(new Text("w3"), new DoubleWritable(w3));
        context.write(new Text("w4"), new DoubleWritable(w4));
    }
}

