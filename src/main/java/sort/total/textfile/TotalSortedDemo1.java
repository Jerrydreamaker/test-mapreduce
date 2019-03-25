package sort.total.textfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

/**
 * 对 Text 文件全局排序
 */
public class TotalSortedDemo1 {

    public static class TemperatureMapper extends Mapper<Text, Text,Text, IntWritable> {
        public TemperatureMapper(){
        }
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String line=key.toString();
            String year=line.substring(15,19);
            String temperatureStr=line.substring(87,92);
            int temperature;
            if(missing(temperatureStr)){
                temperature=Integer.MIN_VALUE;
            }else {
                temperature=Integer.parseInt(temperatureStr);
            }
            context.write(new Text(year),new IntWritable(temperature));
        }

        private boolean missing(String str){
            return str.equals("+9999");
        }
    }

    public static class MaxTemperatureReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        public MaxTemperatureReducer() {

        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            context.write(key,values.iterator().next());
        }
    }

    public static void main(String[] args) throws Exception {
        //Configuration conf = new Configuration();
        Job job = Job.getInstance();
        Configuration conf=job.getConfiguration();
        //设置 mapper 类
        job.setMapperClass(TemperatureMapper.class);
        //设置 combiner 类
        //job.setCombinerClass(sort.total.sequencefile.TotalSortedDemo.MaxTemperatureReducer.class);
        //设置 reducer 类
        job.setReducerClass(TotalSortedDemo1.MaxTemperatureReducer.class);
        //设置 Reduce 输出 Key 的类型
        job.setOutputKeyClass(Text.class);
        //设置 Reduce 输出 Value 的类型
        job.setOutputValueClass(IntWritable.class);
        //设置 reduce 数
        job.setNumReduceTasks(2);
        // 设置输入文件类型
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        //设置全局有序分区
        job.setPartitionerClass(TotalOrderPartitioner.class);
        //设置文件输入路径。
        InputSampler.RandomSampler<Object, Object> sampler = new InputSampler.RandomSampler<Object, Object>(0.1, 10, 4);
        // 设置 partition 文件写入路径，本地文件系统。
        TotalOrderPartitioner.setPartitionFile(conf,new Path("_partition2.lst"));
        System.out.println(TotalOrderPartitioner.getPartitionFile(conf));
        FileInputFormat.addInputPath(job,
                new Path("/input1"));
        //设置文件输出路径。
        FileOutputFormat.setOutputPath(job,
                new Path("hdfs://127.0.0.1:9000/test/test1.seq"));
        //执行任务。
        InputSampler.writePartitionFile(job,sampler);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
