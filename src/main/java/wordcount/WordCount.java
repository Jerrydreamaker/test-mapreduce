package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * 单词计数
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        //设置 mapper 类
        job.setMapperClass(TokenizerMapper.class);
        //设置 combiner 类
        job.setCombinerClass(IntSumReducer.class);
        //设置 reducer 类
        job.setReducerClass(IntSumReducer.class);
        //设置 Reduce 输出 Key 的类型
        job.setOutputKeyClass(Text.class);
        //设置 Reduce 输出 Value 的类型
        job.setOutputValueClass(IntWritable.class);
        //设置分区器类型。
        job.setPartitionerClass(MyPartitioner.class);
        //设置文件输入路径。
        FileInputFormat.addInputPath(job,
                new Path("/Users/dreamaker/Downloads/hadoop-2.7.1/input"));
        //设置文件输出路径。
        FileOutputFormat.setOutputPath(job,
                new Path("/Users/dreamaker/Downloads/hadoop-2.7.1/output"));
        //执行任务。
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public IntSumReducer() {
        }

        //key 是 word,Value 是 词频的 Iterable 对象。
        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            IntWritable val;
            for(Iterator i$ = values.iterator(); i$.hasNext(); sum += val.get()) {
                val = (IntWritable)i$.next();
            }
            this.result.set(sum);
            context.write(key, this.result);
        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        //key 是文件的偏移，Value 是一行的内容。
        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()) {
                this.word.set(itr.nextToken());
                context.write(this.word, one);
            }
        }
    }
}
