package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GradientReducerTask3 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private double m1;
    private double m2;
    private double m3;
    private double m4;
    private double b;
    private double learningRate;
    private double m1Sum = 0.0;
    private double m2Sum = 0.0;
    private double m3Sum = 0.0;
    private double m4Sum = 0.0;
    private double bSum = 0.0;
    private double cost = 0.0;
    private double count = 0;

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        m1 = Double.parseDouble(conf.get("m1"));
        m2 = Double.parseDouble(conf.get("m2"));
        m3 = Double.parseDouble(conf.get("m3"));
        m4 = Double.parseDouble(conf.get("m4"));
        b = Double.parseDouble(conf.get("b"));
        learningRate = Double.parseDouble(conf.get("learningRate"));
        System.out.println("Learning rate: " + learningRate);
        
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }

        switch (key.toString()) {
            case "m1Gradient":
                m1Sum = sum;
                break;
            case "m2Gradient":
                m2Sum = sum;
                break;
            case "m3Gradient":
                m3Sum = sum;
                break;
            case "m4Gradient":
                m4Sum = sum;
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
    public void cleanup(Context context) throws IOException, InterruptedException {
        m1 -= learningRate * (2 * m1Sum) / count;
        m2 -= learningRate * (2 * m2Sum) / count;
        m3 -= learningRate * (2 * m3Sum) / count;
        m4 -= learningRate * (2 * m4Sum) / count;
        b -= learningRate * (2 * bSum) / count;

        cost = cost / count;

        // Write updated parameters and cost to sequence file
        writeParamsToSequenceFile(context, m1, m2, m3, m4, b, cost);
        context.write(new Text("m1"), new DoubleWritable(m1));
        context.write(new Text("m2"), new DoubleWritable(m2));
        context.write(new Text("m3"), new DoubleWritable(m3));
        context.write(new Text("m4"), new DoubleWritable(m4));
        context.write(new Text("b"), new DoubleWritable(b));
        context.write(new Text("cost"), new DoubleWritable(cost));
    }

    private void writeParamsToSequenceFile(Context context, double m1, double m2, double m3, double m4, double b, double cost) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        Path filePath = new Path(FileOutputFormat.getOutputPath(context), "m_b_values.seq");

        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, filePath, Text.class, DoubleWritable.class);

        try {
            writer.append(new Text("m1"), new DoubleWritable(m1));
            writer.append(new Text("m2"), new DoubleWritable(m2));
            writer.append(new Text("m3"), new DoubleWritable(m3));
            writer.append(new Text("m4"), new DoubleWritable(m4));
            writer.append(new Text("b"), new DoubleWritable(b));
            writer.append(new Text("cost"), new DoubleWritable(cost));
            System.out.println("Reducer, printing m1, m2, m3, m4, b, cost: " + m1 + ", " + m2 + ", " + m3 + ", " + m4 + ", " + b + ", " + cost);
        } finally {
            writer.close();
        }
    }
}
