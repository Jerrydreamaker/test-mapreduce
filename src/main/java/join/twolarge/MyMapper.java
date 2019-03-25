package join.twolarge;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text,CombineKey,Text> {
    @Override
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String line=value.toString();
        int flag=0;
        int cid;
        FileSplit split=(FileSplit) context.getInputSplit();
        String path=split.getPath().toString();
        if(path.contains("customers")){
            flag=0;
            cid=Integer.parseInt(line.split("\t")[0]);
        }else {
            flag=1;
            cid=Integer.parseInt(line.split("\t")[3]);
        }
        context.write(new CombineKey(flag,cid),value);
    }
}
