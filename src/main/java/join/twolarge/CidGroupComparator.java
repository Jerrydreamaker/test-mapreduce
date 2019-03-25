package join.twolarge;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CidGroupComparator extends WritableComparator {
    public CidGroupComparator(){
        super(CombineKey.class,true);
    }
    @Override
    public int compare(WritableComparable c1, WritableComparable c2){
        return ((CombineKey)c1).getCid()-((CombineKey)c2).getCid();
    }
}
