package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, FloatWritable> {

	private Text MapKey = new Text();
	private FloatWritable MapValue = new FloatWritable();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		//Split data
		String[] data = value.toString().split(",");

		//clean up step. true if valid line, false if invalid line.
		if(cleanUpData(data)) {
			float tripDist = Float.parseFloat(data[5]);
			float fareAmount = Float.parseFloat(data[11]);

			//sum of trip distance
			MapKey.set("SUM_X");
			MapValue.set(tripDist);
			context.write(MapKey, MapValue);

			//sum of fare amount
			MapKey.set("SUM_Y");
			MapValue.set(fareAmount);
			context.write(MapKey, MapValue);

			//sum of product of trip dist and fare amount
			MapKey.set("SUM_XY");
			MapValue.set(tripDist * fareAmount);
			context.write(MapKey, MapValue);

			//sum of trip distance ^ 2
			MapKey.set("SUM_X2");
			MapValue.set(tripDist * tripDist);
			context.write(MapKey, MapValue);

			MapKey.set("COUNT");
			MapValue.set(1);
			context.write(MapKey, MapValue);
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