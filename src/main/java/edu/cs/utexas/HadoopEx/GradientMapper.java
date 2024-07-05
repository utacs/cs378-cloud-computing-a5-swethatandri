package edu.cs.utexas.HadoopEx;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GradientMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    public double m; 
    public double b;
    private Text MapKey = new Text();
	private DoubleWritable MapValue = new DoubleWritable();

    /**
     * Helper set up method to pass in predicted m and b values between iterations
     * @param context context we're writing on
     */
    @Override
    public void setup(Context context) {
        //Get the updated val of m and b, initially 0.001
        Configuration conf = context.getConfiguration();
        m = Double.parseDouble(conf.get("m"));
        b = Double.parseDouble(conf.get("b"));
    }

    /**
     * Method to map calculated error values prior to summation
     * @param key the respective key for the map we are adding to
     * @param value the respective value pair for the map we are adding to
     * @param context context we're writing on
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if(cleanUpData(fields)) {
            double x = Double.parseDouble(fields[5]); // trip distance
            double y = Double.parseDouble(fields[11]); // fare amount

            //Checking to see if the m and b updated correctly after the reducer. should initially be 0.001.
            //System.out.println("DEBUGGING in mapper inital val: M = " + m + "B = " + b);
            
            // calculates the shared error portion for partials
            // evaluates to all 0's for testing.csv
            double error = y - ((m * x) + b);

            // calculates individual partial deriv formulas (before applying sum and outside operations)
            // adds to mapper
            double mGradient = -x * (error);
            MapKey.set("mGradient");
            MapValue.set(mGradient);
            context.write(MapKey, MapValue);

            double bGradient = -(error);
            MapKey.set("bGradient");
            MapValue.set(bGradient);
            context.write(MapKey, MapValue);

            // cost function
            MapKey.set("cost");
            MapValue.set(error * error);
            context.write(MapKey, MapValue);

            // logs each entry as count (N) val
            MapKey.set("COUNT");
            MapValue.set(1.0);
            context.write(MapKey, MapValue);
        }
    }

    /**
     * Helper method to clean up invalid lines of data based on the assignment requirements.
     * @param data array of all the 17 attributes
     * @return true if it's a valid line, false otherwise.
     */
	private boolean cleanUpData(String[] data) {
		try {
			//trip time less than 2 min or greater than 1 hr
			int trip_time = Integer.parseInt(data[4]);
			if(trip_time < 120 || trip_time > 3600) {
				return false;
			}

			//fare_amount less than $3 or greater than $200
			float fare_amount = Float.parseFloat(data[11]);
			if(fare_amount < 3.0 || fare_amount > 200.0) {
				return false;
			}

			//trip_dist less than 1 mi or greater than 50 mi
			float trip_dist = Float.parseFloat(data[5]);
			if(trip_dist < 1.0 || trip_dist > 50.0) {
				return false;
			}

			//toll amount less than $3
			float toll_amt = Float.parseFloat(data[15]);
			if(toll_amt < 3.0) {
				return false;
			}

		} catch(NumberFormatException e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}
}