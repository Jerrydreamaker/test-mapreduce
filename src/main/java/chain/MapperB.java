package chain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperB extends Mapper<Text, IntWritable,Text,IntWritable> {
    @Override
    public void map(Text key,IntWritable value,
                    Context context) throws IOException, InterruptedException {
        context.write(key,value);
        context.getCounter("m","MB").increment(1);
    }
}
