package sort.total.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import java.io.IOException;

/**
 * 全局排序
 */
public class TotalSortedDemo {
    public static class MaxTemperatureMapper
            extends Mapper<IntWritable, Text,IntWritable, Text> {

        public MaxTemperatureMapper(){
        }
        @Override
        public void map(IntWritable key,Text value,Mapper<IntWritable, Text,IntWritable, Text>.Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }
    public static class MaxTemperatureReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
        public MaxTemperatureReducer() {

        }

        public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            context.write(key,values.iterator().next());
        }
    }

    public static void main(String[] args) throws Exception {
        //Configuration conf = new Configuration();
        Job job = Job.getInstance();
        Configuration conf=job.getConfiguration();
        //设置 mapper 类
        job.setMapperClass(TotalSortedDemo.MaxTemperatureMapper.class);
        //设置 reducer 类
        job.setReducerClass(TotalSortedDemo.MaxTemperatureReducer.class);
        //设置 Reduce 输出 Key 的类型
        job.setOutputKeyClass(IntWritable.class);
        //设置 Reduce 输出 Value 的类型
        job.setOutputValueClass(Text.class);
        //设置 reduce 数
        job.setNumReduceTasks(2);
        // 设置输入文件类型
        job.setInputFormatClass(SequenceFileInputFormat.class);
        //设置全局有序分区
        job.setPartitionerClass(TotalOrderPartitioner.class);
        //设置文件输入路径。
        InputSampler.RandomSampler<IntWritable, IntWritable> sampler = new InputSampler.RandomSampler<IntWritable, IntWritable>(0.1, 10, 4);
        // 设置 partition 文件写入路径，本地文件系统。
        TotalOrderPartitioner.setPartitionFile(conf,new Path("_partition2.lst"));
        FileInputFormat.addInputPath(job,
                new Path("hdfs://127.0.0.1:9000/test/test.seq"));
        //设置文件输出路径。
        FileOutputFormat.setOutputPath(job,
                new Path("hdfs://127.0.0.1:9000/test/test1.seq"));
        InputSampler.writePartitionFile(job,sampler);
        //执行任务。
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
