package edu.cs.utexas.HadoopEx;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GradientMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    //should this be updated with the predicted m and b?
    public double m; 
    public double b;
    private Text MapKey = new Text();
	private DoubleWritable MapValue = new DoubleWritable();

    @Override
    public void setup(Context context) {
        //Get the updated val of m and b, initially 0.001
        Configuration conf = context.getConfiguration();
        m = Double.parseDouble(conf.get("m"));
        b = Double.parseDouble(conf.get("b"));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        double x = Double.parseDouble(fields[5]); // trip distance
        double y = Double.parseDouble(fields[11]); // fare amount

        //Checking to see if the m and b updated correctly after the reducer. should initially be 0.001.
        //System.out.println("DEBUGGING in mapper inital val: M = " + m + "B = " + b);
        
        // calculates the shared error portion for partials
        // evaluates to all 0's for testing.csv
        double error = y - ((m * x) + b);

        // calculates individual partial formulas (before applying sum and outside operations)
        // adds to mapper
        double mGradient = -x * (error);
        MapKey.set("mGradient");
		MapValue.set(mGradient);
        context.write(MapKey, MapValue);

        double bGradient = -(error);
        MapKey.set("bGradient");
		MapValue.set(bGradient);
        context.write(MapKey, MapValue);

        MapKey.set("cost");
		MapValue.set(error * error);
		context.write(MapKey, MapValue);

        MapKey.set("COUNT");
		MapValue.set(1.0);
		context.write(MapKey, MapValue);
    }
}