package com.voidxin.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text,IntWritable> {
    private static final Integer ERROR_TEMPER = 9999;
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		//获取year
		String year = line.substring(15,19);
		//获取温度
		Integer airTemperature;
		if(line.charAt(87) == '+') {
			airTemperature = Integer.parseInt(line.substring(88,92));
			
		} else {
			airTemperature = Integer.parseInt(line.substring(87,92));
		}
		String quality = line.substring(92,93);
		
		if(airTemperature != ERROR_TEMPER && quality.matches("[01459]")) {
			context.write(new Text(year), new IntWritable(airTemperature));
		}
	}

}
