package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultipleRegMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private static final int NUM_VARIABLES = 4;
    private double initVal = 0.1;
	private double learningRate = 0.001;

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
        //extract the data
		String[] data = value.toString().split(",");
        
        //to hold all the variables
        double[] variables = new double[NUM_VARIABLES];
        double y = 0.0; // target variable(total_amount)

        try{
            //Get all variables needed.
            variables[0] = Double.parseDouble(data[4]); // trip time
            variables[1] = Double.parseDouble(data[5]); // trip dist
            variables[2] = Double.parseDouble(data[11]); // fare amount
            variables[3] = Double.parseDouble(data[15]); //tolls amount
            y = Double.parseDouble(data[16]); //total amount
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return;
        }

        //Calculated based on current variable values (updated with each iteration)
        double predVal = calculatePredVal(variables, context);
        double error = predVal - y;//(predicted value - actual value)

        //find gradient for each variable.
	}

    private double calculatePredVal(double[] variables, Context context) {
        //Current var vals from context
        double[] predVals = new double[NUM_VARIABLES];

        //get the current predVal from context
        double predictedVal = 0.0;
        
        //Calculate the predicted value using predVal (initially 0.1) and actual values from variables.
        //predVal0 + predVal1*variables1 + ... predVal3*variables3

        return predictedVal;
    }
}