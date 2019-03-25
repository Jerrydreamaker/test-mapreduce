package sort.second;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTempSecReducer extends Reducer<CombineKey, NullWritable,
        CombineKey,NullWritable> {
    @Override
    public void reduce(CombineKey key,Iterable<NullWritable> values,
                       Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
