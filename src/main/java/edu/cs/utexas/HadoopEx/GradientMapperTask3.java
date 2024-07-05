package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//Mapper class for task 3: Finding multiple linear regression using fradient descent 
public class GradientMapperTask3 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    // Holds all the "predicted" parameter values with each iteration(initally all 0.1).
    private double[] params;

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        params = new double[5];
        // get all param vals from the conf set in driver.
        for(int i = 0; i < params.length; i++) {
            params[i] = Double.parseDouble(conf.get("w" + i));
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the data line read from the file.
        String[] fields = value.toString().split(",");

        if (cleanUpData(fields)) {
            // Create array to hold the actual data read from file.
            double[] actualData = new double[4];
            // insert all necessary variables: trip_time, trip_dist, fare_amt, toll_amt
            actualData[0] = Double.parseDouble(fields[4]); 
            actualData[1] = Double.parseDouble(fields[5]); 
            actualData[2] = Double.parseDouble(fields[11]);
            actualData[3] = Double.parseDouble(fields[15]); 
            // Total amount: the value we want to predict
            double y = Double.parseDouble(fields[16]);

            // Find the prediction with: predY = w0 + w1x1 + w2x2 ... wnxn
            double pred = params[0]; 
            for(int i = 1; i <= actualData.length; i++) {
                pred += params[i] * actualData[i-1];
            }

            // Calculate difference between predY and actual y value
            double error = y - pred;

            context.write(new Text("Count"), new DoubleWritable(1.0));
            context.write(new Text("Cost"), new DoubleWritable(error * error));

            // bias, "b val for t3".
            context.write(new Text("w0"), new DoubleWritable(error)); 
            //compute the partial derivates for each parameter before the summation (xi*(actual y - pred y))
            for(int i = 1; i <= actualData.length; i++) {
                context.write(new Text("w" + i), new DoubleWritable(error * actualData[i-1]));
            }
        }
    }

    /**
     * Helper method to clean up invalid lines of data based on the assignment requirements.
     * @param data array of all the 17 attributes
     * @return true if it's a valid line, false otherwise.
     */
    private boolean cleanUpData(String[] data) {

        //Additional check: if there is 17 attributes.
        if(data.length != 17) {
            //Invalid line
            return false;
        }

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


