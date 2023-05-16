package bigdata;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
public class TopK {
	// 직원 정보를 레코드화 => 객체로 저장할 예정
	public static class Emp {
		public int id;
		public int salary;
		public String dept_id;
		public String emp_info;

		public Emp(int _id, int _salary, String _dept_id, String _emp_info) {
			this.id = _id;
			this.salary = _salary;
			this.dept_id = _dept_id;
			this.emp_info = _emp_info;
		}
		public String getString()
		{
			return id + "|" + dept_id + "|" + salary + "|" + emp_info;
		}
	}
	
	public static class EmpComparator implements Comparator<Emp>{
		public int compare(Emp x, Emp y) { // salary 기준으로 내림차순(큰 순서대로) => Queue의 head에 salary가 가장 낮은 Emp가 위치함
			if(x.salary > y.salary) return 1;
			if(x.salary < y.salary) return -1;
			return 0;
		}
	}
	// 직원 정보를 queue에 넣을지 말지를 결정하는 메소드
	public static void insertEmp(PriorityQueue q, int id, int salary, String dept_id, String emp_info, int topK) {
		Emp emp_head = (Emp) q.peek(); // head의 reference 얻어오기 (head 뺀 것 x)
		if(q.size() < topK || emp_head.salary < salary) { // queue에 topK개보다 적은 원소가 있거나, queue의 head emp의 salary가 더 작으면
			Emp emp = new Emp(id, salary, dept_id, emp_info);
			q.add(emp); // 새 원소 집어넣기
			if(q.size() > topK) q.remove(); // queue가 topK개 넘어서면 head 제거
		}
	}
	
	public static class TopKMapper extends Mapper<Object,Text,Text,NullWritable>{ //NullWritable은 길이가 0인 no-data를 의미. key or value가 의미 없을 때 사용
		private PriorityQueue<Emp> queue;
		private Comparator<Emp> comp = new EmpComparator();
		private int topK;
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			
			// 레코드에서 각 필드 추출
			int emp_id = Integer.parseInt(itr.nextToken().trim());
			String dept_id = itr.nextToken().trim();
			int salary = Integer.parseInt(itr.nextToken().trim());
			String emp_info = itr.nextToken().trim();
				
			insertEmp(queue, emp_id, salary, dept_id, emp_info, topK); // 직접 emit (x) , buffer에 insert
		}
		
		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK" , -1);
			queue = new PriorityQueue<Emp> (topK, comp); //comp 기준으로 size가 topK인 queue 생성
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{
			while(queue.size() != 0) { //queue가 0될 때까지
				Emp emp = (Emp) queue.remove(); //head를 빼서
				context.write(new Text(emp.getString()), NullWritable.get()); //emit
			}
		}
	}
	
	public static class TopKReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
		private PriorityQueue<Emp> queue;
		private Comparator<Emp> comp = new EmpComparator();
		private int topK;
		
		public void reduce(Text key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(key.toString(), "|"); // value가 아니라 key를 토큰화
			
			// 그 외는 전부 mapper와 동일
			int emp_id = Integer.parseInt(itr.nextToken().trim());
			String dept_id = itr.nextToken().trim();
			int salary = Integer.parseInt(itr.nextToken().trim());
			String emp_info = itr.nextToken().trim();
				
			insertEmp(queue, emp_id, salary, dept_id, emp_info, topK); // 직접 emit (x) , buffer에 insert
		}
		
		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK" , -1);
			queue = new PriorityQueue<Emp> (topK, comp); //comp 기준으로 size가 topK인 queue 생성
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{
			while(queue.size() != 0) { //queue가 0될 때까지
				Emp emp = (Emp) queue.remove(); //head를 빼서
				context.write(new Text(emp.getString()), NullWritable.get()); //emit
			}
		}
	}
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		int topK = 3; // 상위 3개
		if(otherArgs.length != 2){
			System.err.println("Usage: TopK <in> <out>"); 
   			System.exit(2);
		}
		conf.setInt("topK", topK);
		
		Job job = new Job(conf, "TopK");
		job.setJarByClass(TopK.class);
		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class); 
		job.setNumReduceTask(1); // reduce task 1개만 띄움 (어차피 test data는 작으니까 reduce 1개만 필요)
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
