package chain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ReduceA extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    public void reduce(Text key,Iterable<IntWritable> values
                    ,Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> ite=values.iterator();
        int sum=0;
        while (ite.hasNext()){
            sum+=ite.next().get();
        }
        context.write(key,new IntWritable(sum));
        context.getCounter("r","RA").increment(1);
    }
}
