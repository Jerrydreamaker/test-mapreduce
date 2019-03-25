package join.onesmall;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class JoinDemo01 {
    public static class MyMapper extends Mapper<LongWritable, Text,
            IntWritable,Text>{
        @Override
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[] orderInfo=line.split("\t");
            int cid=Integer.parseInt(orderInfo[3]);
            context.write(new IntWritable(cid),value);
        }
    }
    public static class MyReducer extends Reducer<IntWritable,Text,
            Text, NullWritable>{
        private Map<Integer,String> map=new HashMap<Integer, String>();
        @Override
        /**
         * setup 方法将 cumstomer 数据缓存进 map。
         */
        public void setup(Context context) throws IOException {
            Configuration configuration=context.getConfiguration();
            FileSystem fs=FileSystem.get(configuration);
            FSDataInputStream fis=fs.open(new Path(configuration.get("customersdir")));
            BufferedReader reader=new BufferedReader(new InputStreamReader(fis));
            String line;
            while ((line=reader.readLine())!=null){
                map.put(new Integer(line.split("\t")[0]),line);
            }
            reader.close();
            //fis.close();
        }
        @Override
        public void reduce(IntWritable key,Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            int cid=key.get();
            for (Text order:values){
                String customerInfo=map.get(cid);
                context.write(new Text(order.toString()+" "+customerInfo),NullWritable.get());
            }
        }

    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("customersdir","/input2/customers");
        Job job = Job.getInstance(conf, "join demo01");
        //设置 mapper 类
        job.setMapperClass(MyMapper.class);
        //设置 reducer 类
        job.setReducerClass(MyReducer.class);
        //设置 Map 输出 Key 的类型
        job.setMapOutputKeyClass(IntWritable.class);
        //设置 Map 输出 Value 的类型
        job.setMapOutputValueClass(Text.class);
        //设置 Reduce 输出 Key 的类型
        job.setOutputKeyClass(Text.class);
        //设置 Reduce 输出 Value 的类型
        job.setOutputValueClass(NullWritable.class);
        //设置 reduce 数。
        job.setNumReduceTasks(2);
        //job.setPartitionerClass(MyPartitioner.class);
        //设置文件输入路径。
        FileInputFormat.addInputPath(job,
                new Path("/input3"));
        //设置文件输出路径。
        FileOutputFormat.setOutputPath(job,
                new Path("/output1"));
        //执行任务。
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
