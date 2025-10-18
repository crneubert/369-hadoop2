package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringJoiner;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class URLListCountry {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            int tab = line.lastIndexOf("\t");
            String left = line.substring(0, tab);

            int slash = left.indexOf('/');
            String country = left.substring(0, slash);
            String url = left.substring(slash);
            context.write(new Text(url), new Text(country));
        }
    }

    public static class ReducerImpl extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text url, Iterable<Text> countries,
                              Context context) throws IOException, InterruptedException {
            TreeSet<String> set = new TreeSet<>();

            for (Text c : countries){
                String s = c.toString();
                set.add(s);
            }
            StringJoiner sj = new StringJoiner(", ");
            for (String c : set) sj.add(c);
            context.write(url, new Text(sj.toString()));
        }
    }

}
