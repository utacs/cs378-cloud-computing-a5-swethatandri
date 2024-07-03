package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, FloatWritable, Text, FloatWritable> {

    private float sumX = 0;
	private float sumY = 0;
	private float sumXY = 0;
	private float sumX2 = 0;
	private float count = 0;

    public void reduce(Text text, Iterable<FloatWritable> values, Context context)
           throws IOException, InterruptedException {
	   
       float sum = 0;
       
       //Add up value that has matching keys.
       for (FloatWritable value : values) {
           sum += value.get();
       }
       
       //accumulate sum based on the key received from the mapper
       switch (text.toString()) {
            case "SUM_X":
                sumX = sum;
                break;
            case "SUM_Y":
                sumY = sum;
                break;
            case "SUM_XY":
                sumXY = sum;
                break;
            case "SUM_X2":
                sumX2 = sum;
                break;
            case "COUNT":
                count = sum;
                break;
        }
    }

    @Override
    // Calculate the slope variable(m) and the y-intercept variable(b) using linear regression formula.
    public void cleanup(Context context) throws IOException, InterruptedException {
        //calculate the m and b values.
        float numeratorM = (count * sumXY) - (sumX * sumY);
        float numeratorB = (sumY * sumX2) - (sumX * sumXY);
        float denom = (count * sumX2) - (sumX * sumX);
        
        float m = numeratorM / denom;
        float b = numeratorB / denom;

        //Write calculated slope and intercept to context
        context.write(new Text("variable m"), new FloatWritable(m));
        context.write(new Text("variable b"), new FloatWritable(b));
    }
}