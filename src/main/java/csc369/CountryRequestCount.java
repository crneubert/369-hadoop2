package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountryRequestCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for country file
    public static class UserMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context)  throws IOException, InterruptedException {
            String host = key.toString().trim();
            String country = value.toString().trim();
            if (host.isEmpty() || country.isEmpty()) return;
            context.write(new Text(host), new Text("C\t" +  country));
        }
    }

    // Mapper for access log file
    public static class MessageMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text one = new Text("L\t1");
        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] sa = value.toString().split(" ");
            Text hostname = new Text();
            hostname.set(sa[0]);
            context.write(hostname, one);
        }
    }


    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
            String country = "Unknown";
            int count = 0;

            for (Text i : values){
                String str = i.toString();
                if (str.startsWith("C\t")){
                    country = str.substring(2).trim();
                } else if (str.startsWith("L\t")){
                    count++;
                }
            }

            if (count > 0){
                context.write(new Text(country), new Text(Integer.toString(count)));
            }
        }
    }


}
