package max;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * 计算每年的最高温度，key 为 year,同一年的键值对发给同一个 reduce。
 */
public class MaxTemperature {
    // map 方法输入为
    // 字符串偏移 ：LongWritable
    // 一行文本：Text
    // 输出为：
    // year: Text
    // temperature : IntWritable
    public static class MaxTemperatureMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        // 使用 @Override 注解，确保覆盖了 Mapper 中定义的 map 方法。
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String year=line.substring(15,19);
            String temperatureStr=line.substring(87,92);
            int temperature;
            if(!missing(temperatureStr)){
                temperature=Integer.parseInt(temperatureStr);
                context.write(new Text(year),new IntWritable(temperature));
            }
        }

        private boolean missing(String str){
            return str.equals("+9999");
        }
    }

    // reduce 方法输入为
    // year ：Text
    // values：key(year) 相同的 temperature 的 Iterable 集合。
    // 输出为：
    // year: Text
    // 某一 key(year) 下的 temperature 的最大值: IntWritable
    public static class MaxTemperatureReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        int maxValue=Integer.MIN_VALUE;
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            while (values.iterator().hasNext()){
                maxValue=Math.max(values.iterator().next().get(),maxValue);
            }
            context.write(key,new IntWritable(maxValue));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "max temperature");
        //设置 mapper 类
        job.setMapperClass(MaxTemperatureMapper.class);
        //设置 combiner 类
        job.setCombinerClass(MaxTemperatureReducer.class);
        //设置 reducer 类
        job.setReducerClass(MaxTemperatureReducer.class);
        //设置 Reduce 输出 Key 的类型
        job.setOutputKeyClass(Text.class);
        //设置 Reduce 输出 Value 的类型
        job.setOutputValueClass(IntWritable.class);
        //设置 reduce 数
        job.setNumReduceTasks(2);
        //设置文件输入路径。
        FileInputFormat.addInputPath(job,
                new Path("/Users/dreamaker/Downloads/hadoop-2.7.1/input1"));
        //设置文件输出路径。
        FileOutputFormat.setOutputPath(job,
                new Path("/Users/dreamaker/Downloads/hadoop-2.7.1/output1"));
        //执行任务。
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
