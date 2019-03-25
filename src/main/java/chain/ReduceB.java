package chain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ReduceB extends Reducer<Text, IntWritable,
        Text,IntWritable> {
    @Override
    public void reduce(Text key,Iterable<IntWritable> values
    ,Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> ite=values.iterator();
        while (ite.hasNext()){
            context.write(key,ite.next());
        }
        context.getCounter("r","RB").increment(1);

    }
}
