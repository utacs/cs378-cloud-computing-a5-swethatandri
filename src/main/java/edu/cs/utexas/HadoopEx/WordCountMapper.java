package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, FloatWritable, FloatWritable> {

	private FloatWritable trip_dist_x = new FloatWritable();
	private FloatWritable fare_amount_y = new FloatWritable();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		//Split data
		String[] data = value.toString().split(",");

		//clean up step. true if valid line, false if invalid line.
		if(cleanUpData(data)) {
			//set trip_distance and fare_amount as floatwritable
			trip_dist_x.set(Float.parseFloat(data[5]));
			fare_amount_y.set(Float.parseFloat(data[11]));

			//Write to context
			context.write(trip_dist_x, fare_amount_y);
		}
	}

	/*
	 * helper method to clean up data. Return true if the line doesn't have any errors,
	 * false if there is.
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