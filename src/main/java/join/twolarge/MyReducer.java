package join.twolarge;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


public class MyReducer extends Reducer<CombineKey, Text,Text, NullWritable> {
    @Override
    public void reduce(CombineKey key,Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
        Iterator<Text> ite=values.iterator();
        String customerInfo=((Iterator) ite).next().toString();
        while (ite.hasNext()){
            context.write(new Text(ite.next().toString().toString()
                    +" "+customerInfo),NullWritable.get());
        }

    }
}
