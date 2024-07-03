package edu.cs.utexas.HadoopEx;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GradientMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private double m = 0.0;
    private double b = 0.0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        double x = Double.parseDouble(fields[5]); // trip distance
        double y = Double.parseDouble(fields[11]); // fare amount

        double prediction = m * x + b;
        double error = y - prediction;

        context.write(new Text("mGradient"), new DoubleWritable(-2 * x * error));
        context.write(new Text("bGradient"), new DoubleWritable(-2 * error));
    }
}