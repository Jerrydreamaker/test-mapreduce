package join.twolarge;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CidPartitioner extends Partitioner<CombineKey,Text> {
    public int getPartition(CombineKey key, Text value, int  numPartitions) {
        return key.getCid()%numPartitions;
    }
}
