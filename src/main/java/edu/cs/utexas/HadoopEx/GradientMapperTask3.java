package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GradientMapperTask3 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private double m1;
    private double m2;
    private double m3;
    private double m4;
    private double b;

    private Text MapKey = new Text();
    private DoubleWritable MapValue = new DoubleWritable();

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        m1 = Double.parseDouble(conf.get("m1"));
        m2 = Double.parseDouble(conf.get("m2"));
        m3 = Double.parseDouble(conf.get("m3"));
        m4 = Double.parseDouble(conf.get("m4"));
        b = Double.parseDouble(conf.get("b"));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (cleanUpData(fields)) {
            //double x0 = 1.0; // Bias term
            double x1 = Double.parseDouble(fields[4]); // Trip time in seconds
            double x2 = Double.parseDouble(fields[5]); // Trip distance in miles
            double x3 = Double.parseDouble(fields[11]); // Fare amount in dollars
            double x4 = Double.parseDouble(fields[15]); // Tolls amount in dollars
            double y = Double.parseDouble(fields[16]); // Total amount paid in dollars

            double prediction = m1 * x1 + m2 * x2 + m3 * x3 + m4 * x4 + b;
            double error = y - prediction;

            MapKey.set("m1Gradient");
            MapValue.set(-x1 * error);
            context.write(MapKey, MapValue);

            MapKey.set("m2Gradient");
            MapValue.set(-x2 * error);
            context.write(MapKey, MapValue);

            MapKey.set("m3Gradient");
            MapValue.set(-x3 * error);
            context.write(MapKey, MapValue);

            MapKey.set("m4Gradient");
            MapValue.set(-x4 * error);
            context.write(MapKey, MapValue);

            MapKey.set("bGradient");
            MapValue.set(-1 * error);
            context.write(MapKey, MapValue);

            MapKey.set("cost");
            MapValue.set(error * error);
            context.write(MapKey, MapValue);

            MapKey.set("COUNT");
            MapValue.set(1.0);
            context.write(MapKey, MapValue);
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


