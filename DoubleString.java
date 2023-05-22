import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

public class DoubleString implements WritableComparable{
	String joinKey = new String();
	String tableName = new String();
	public DoubleString(String joinKey, String tableName) {
		super();
		this.joinKey = joinKey;
		this.tableName = tableName;
	}
	public DoubleString() {
	}
	
	public void readFields(DataInput in) throws IOException{
		joinKey = in.readUTF();
		tableName = in.readUTF();
	}
	
	public void write(DataOutput out) throws IOException{
		out.writeUTF(joinKey);
		out.writeUTF(tableName);
	}
	
	public int compareTo(Object o1) {
		DoubleString o = (DoubleString) o1;
		
		int ret = joinKey.compareTo(o.joinKey);
		if(ret != 0) return ret;
		return -1 * tableName.compareTo(o.tableName);
	}
	
	public String toString() {
		return joinKey + " " + tableName;
	}
}
