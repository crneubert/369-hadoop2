package csc369;

import java.io.*;
import org.apache.hadoop.io.*;

public class CountryCountPair implements WritableComparable<CountryCountPair> {
    private Text country = new Text();
    private IntWritable negCount = new IntWritable();

    public CountryCountPair() {}
    public CountryCountPair(String c, int count) {
        this.country.set(c);
        this.negCount.set(-count);
    }

    public Text getCountry() { return country; }
    public IntWritable getNegCount() { return negCount; }

    @Override public void write(DataOutput out) throws IOException { country.write(out); negCount.write(out); }
    @Override public void readFields(DataInput in) throws IOException { country.readFields(in); negCount.readFields(in); }

    @Override
    public int compareTo(CountryCountPair o) {
        int c = country.compareTo(o.country);
        if (c != 0) return c;
        return Integer.compare(negCount.get(), o.negCount.get());
    }

}

