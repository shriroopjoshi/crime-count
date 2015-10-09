/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package crimecount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author shriroop_joshi
 */
public class DistrictCrimeMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {

    @Override
    public void map(Object key, Text value, OutputCollector<Text, IntWritable> oc, Reporter rprtr)
            throws IOException {
        //int i = 0;
        String district = value.toString().split(",")[2];
//        StringTokenizer tokenizer = new StringTokenizer(line, ",");
//        while (tokenizer.hasMoreTokens()) {
//            if (i++ == 2) {
//                oc.collect(new Text(tokenizer.nextToken()), new IntWritable(1));
//            }
//        }
        oc.collect(new Text(district), new IntWritable(1));
    }

}
