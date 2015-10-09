/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package crimecount;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author shriroop_joshi
 */
public class DistrictCrimeReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterator<IntWritable> valueItr, OutputCollector<Text, IntWritable> oc, Reporter rprtr)
            throws IOException {
        int crimes = 0;
        while(valueItr.hasNext()) {
            crimes += valueItr.next().get();
        }
        oc.collect(key, new IntWritable(crimes));
    }
    
}
