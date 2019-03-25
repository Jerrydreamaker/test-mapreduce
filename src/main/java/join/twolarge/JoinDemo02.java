package join.twolarge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JoinDemo02 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "join demo02");
        //设置 mapper 类
        job.setMapperClass(MyMapper.class);
        //设置 reducer 类
        job.setReducerClass(MyReducer.class);
        job.setMapOutputValueClass(Text.class);
        //设置 Reduce 输出 Key 的类型
        job.setOutputKeyClass(CombineKey.class);
        //设置 Reduce 输出 Value 的类型
        job.setOutputValueClass(NullWritable.class);
        //设置分区器类型。
        job.setPartitionerClass(CidPartitioner.class);
        //设置分组器，值相同的记录作为同一个 Iterable 容器发给 Reduce
        job.setGroupingComparatorClass(CidGroupComparator.class);
        // 设置 Reduce 数
        job.setNumReduceTasks(2);
        //设置文件输入路径。
        FileInputFormat.addInputPath(job,
                new Path("/input2"));
        //设置文件输出路径。
        FileOutputFormat.setOutputPath(job,
                new Path("/output1"));
        //执行任务。
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
