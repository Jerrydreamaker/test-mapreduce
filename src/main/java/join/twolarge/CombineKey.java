package join.twolarge;

import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CombineKey implements WritableComparable<CombineKey>{
    private int flag;
    private int cid;

    //private static Logger logger=LoggerFactory.getLogger(CombineKey.class);
    public CombineKey() {
    }

    public CombineKey(int flag, int cid) {
        this.flag = flag;
        this.cid = cid;
    }

    public int getCid() {
        return cid;
    }

    public int compareTo(CombineKey o) {
        if(flag==o.flag){
            // 再按cid升序排序
            return cid-o.cid;
        }else {
            // 首先按 flag 升序排序
            return flag-o.flag;
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(flag);
        dataOutput.writeInt(cid);
    }

    public void readFields(DataInput dataInput) throws IOException {
        flag=dataInput.readInt();
        cid=dataInput.readInt();
    }
    @Override
    public String toString(){
        return flag+" "+cid;
    }
}
