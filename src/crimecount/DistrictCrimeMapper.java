/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package crimecount;

import classes.CrimeRecord;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
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
public class DistrictCrimeMapper extends MapReduceBase implements Mapper<Object, Text, Text, CrimeRecord> {

    @Override
    public void map(Object key, Text value, OutputCollector<Text, CrimeRecord> oc, Reporter rprtr)
            throws IOException {
        //int i = 0;
        String[] fields = value.toString().split(",");

        String datePattern = "dd-MM-yy HH:mm";
        DateFormat df = new SimpleDateFormat(datePattern, Locale.ENGLISH);
//        Date d;
//        try {
//            d = df.parse(fields[0]);
//        } catch (ParseException ex) {
//            d = new Date();
//            Logger.getLogger(DistrictCrimeMapper.class.getName()).log(Level.SEVERE, null, ex);
//        }
        CrimeRecord cr = new CrimeRecord();
        try {
            cr.setCrimeDate(df.parse(fields[0]));
        } catch (ParseException ex) {
            cr.setCrimeDate(new Date());
        }
        cr.setAddress(fields[1]);
        cr.setDistrict(Integer.parseInt(fields[2]));
        cr.setBeat(fields[3]);
        cr.setGrid(Integer.parseInt(fields[4]));
        cr.setDecription(fields[5]);
        cr.setNCIC_code(Integer.parseInt(fields[6]));
        cr.setLatitude(Double.parseDouble(fields[7]));
        cr.setLongitude(Double.parseDouble(fields[8]));
        String time = fields[0].split(" ")[1].split(":")[0];
        switch (time) {
            default:
                break;
            case "00":
                oc.collect(new Text(time), cr);
                break;
            case "01":
                oc.collect(new Text(time), cr);
                break;
            case "02":
                oc.collect(new Text(time), cr);
                break;
            case "03":
                oc.collect(new Text(time), cr);
                break;
            case "04":
                oc.collect(new Text(time), cr);
                break;
            case "05":
                oc.collect(new Text(time), cr);
                break;
            case "06":
                oc.collect(new Text(time), cr);
                break;
            case "07":
                oc.collect(new Text(time), cr);
                break;
            case "08":
                oc.collect(new Text(time), cr);
                break;
            case "09":
                oc.collect(new Text(time), cr);
                break;
            case "10":
                oc.collect(new Text(time), cr);
                break;
            case "11":
                oc.collect(new Text(time), cr);
                break;
            case "12":
                oc.collect(new Text(time), cr);
                break;
            case "13":
                oc.collect(new Text(time), cr);
                break;
            case "14":
                oc.collect(new Text(time), cr);
                break;
            case "15":
                oc.collect(new Text(time), cr);
                break;
            case "16":
                oc.collect(new Text(time), cr);
                break;
            case "17":
                oc.collect(new Text(time), cr);
                break;
            case "18":
                oc.collect(new Text(time), cr);
                break;
            case "19":
                oc.collect(new Text(time), cr);
                break;
            case "20":
                oc.collect(new Text(time), cr);
                break;
            case "21":
                oc.collect(new Text(time), cr);
                break;
            case "22":
                oc.collect(new Text(time), cr);
                break;
            case "23":
                oc.collect(new Text(time), cr);
                break;
        }
//        StringTokenizer tokenizer = new StringTokenizer(line, ",");
//        while (tokenizer.hasMoreTokens()) {
//            if (i++ == 2) {
//                oc.collect(new Text(tokenizer.nextToken()), new IntWritable(1));
//            }
//        }
        //oc.collect(new Text(district), new IntWritable(1));
    }

}
