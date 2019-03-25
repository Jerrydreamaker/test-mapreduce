package chain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperA extends Mapper<LongWritable, Text,
        Text, IntWritable> {
    @Override
    public void map(LongWritable key,Text value,
                    Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] arr=line.split(" ");
        for (String str:arr){
            context.write(new Text(str),new IntWritable(1));
        }
        context.getCounter("m","MA").increment(1);
    }
}
