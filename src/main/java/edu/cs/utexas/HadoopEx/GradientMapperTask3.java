package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GradientMapperTask3 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private double w0;
    private double w1;
    private double w2;
    private double w3;
    private double w4;

    private Text MapKey = new Text();
    private DoubleWritable MapValue = new DoubleWritable();

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        w0 = Double.parseDouble(conf.get("w0"));
        w1 = Double.parseDouble(conf.get("w1"));
        w2 = Double.parseDouble(conf.get("w2"));
        w3 = Double.parseDouble(conf.get("w3"));
        w4 = Double.parseDouble(conf.get("w4"));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (cleanUpData(fields)) {
            double x0 = 1.0; // Bias term
            double x1 = Double.parseDouble(fields[4]); // Trip time in seconds
            double x2 = Double.parseDouble(fields[5]); // Trip distance in miles
            double x3 = Double.parseDouble(fields[11]); // Fare amount in dollars
            double x4 = Double.parseDouble(fields[15]); // Tolls amount in dollars
            double y = Double.parseDouble(fields[16]); // Total amount paid in dollars

            double prediction = w0 * x0 + w1 * x1 + w2 * x2 + w3 * x3 + w4 * x4;
            double error = y - prediction;

            context.write(new Text("w0Gradient"), new DoubleWritable(-2 * x0 * error));
            context.write(new Text("w1Gradient"), new DoubleWritable(-2 * x1 * error));
            context.write(new Text("w2Gradient"), new DoubleWritable(-2 * x2 * error));
            context.write(new Text("w3Gradient"), new DoubleWritable(-2 * x3 * error));
            context.write(new Text("w4Gradient"), new DoubleWritable(-2 * x4 * error));
        }
    }

    private boolean cleanUpData(String[] data) {
        try {
            int trip_time = Integer.parseInt(data[4]);
            if (trip_time < 120 || trip_time > 3600) {
                return false;
            }

            float fare_amount = Float.parseFloat(data[11]);
            if (fare_amount < 3.0 || fare_amount > 200.0) {
                return false;
            }

            float trip_dist = Float.parseFloat(data[5]);
            if (trip_dist < 1.0 || trip_dist > 50.0) {
                return false;
            }

            float toll_amt = Float.parseFloat(data[15]);
            if (toll_amt < 3.0) {
                return false;
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }
}


