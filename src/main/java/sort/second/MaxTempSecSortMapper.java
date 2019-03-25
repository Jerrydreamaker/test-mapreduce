package sort.second;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTempSecSortMapper extends Mapper<LongWritable, Text,
        CombineKey, NullWritable> {
    //public MaxTempSecSortMapper(){}
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        int year=Integer.valueOf(line.substring(15,19));
        String temperatureStr=line.substring(87,92);
        if(!missing(temperatureStr)){
            context.write(new CombineKey(year,Integer.parseInt(temperatureStr)),NullWritable.get());
        }

    }

    private boolean missing(String str){
        return str.equals("+9999");
    }
}
