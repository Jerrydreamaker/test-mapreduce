package chain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        Configuration confA = new Configuration(false);
        ChainMapper.addMapper(job,MapperA.class, LongWritable.class,
                Text.class,Text.class,IntWritable.class,confA);
        Configuration confB = new Configuration(false);
        ChainMapper.addMapper(job,MapperB.class,Text.class,
                IntWritable.class,Text.class,IntWritable.class,confB);
        Configuration confC=new Configuration(false);
        ChainReducer.setReducer(job,ReduceA.class,Text.class,IntWritable.class,
                Text.class,IntWritable.class,confC);
        Configuration confD=new Configuration(false);
        ChainReducer.addMapper(job,MapperC.class,Text.class,
                IntWritable.class,Text.class,IntWritable.class,confD);
        /**
         * Reducer 只能设置一次
        Configuration confE=new Configuration(false);
        ChainReducer.setReducer(job,ReduceB.class,Text.class,
                IntWritable.class,Text.class,IntWritable.class,confE);
        */
         //设置文件输入路径。
        FileInputFormat.addInputPath(job,
                new Path("/input4"));
        //设置文件输出路径。
        FileOutputFormat.setOutputPath(job,
                new Path("/output1"));
        //执行任务。
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
