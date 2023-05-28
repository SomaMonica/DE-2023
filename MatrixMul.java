import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MatrixMultiplication {

  public static class MatrixEntry implements WritableComparable<MatrixEntry> {

    private Text matrixName;
    private IntWritable row;
    private IntWritable col;
    private IntWritable value;

    public MatrixEntry() {
      this.matrixName = new Text();
      this.row = new IntWritable();
      this.col = new IntWritable();
      this.value = new IntWritable();
    }

    public MatrixEntry(String matrixName, int row, int col, int value) {
      this.matrixName = new Text(matrixName);
      this.row = new IntWritable(row);
      this.col = new IntWritable(col);
      this.value = new IntWritable(value);
    }

    public Text getMatrixName() {
      return matrixName;
    }

    public IntWritable getRow() {
      return row;
    }

    public IntWritable getCol() {
      return col;
    }

    public IntWritable getValue() {
      return value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      matrixName.write(out);
      row.write(out);
      col.write(out);
      value.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      matrixName.readFields(in);
      row.readFields(in);
      col.readFields(in);
      value.readFields(in);
    }

    @Override
    public int compareTo(MatrixEntry other) {
      int cmp = matrixName.compareTo(other.getMatrixName());
      if (cmp == 0) {
        cmp = row.compareTo(other.getRow());
        if (cmp == 0) {
          cmp = col.compareTo(other.getCol());
        }
      }
      return cmp;
    }

    @Override
    public String toString() {
      return matrixName + "," + row + "," + col + "," + value;
    }
  }

  public static class MatrixMapper extends Mapper<Object, Text, MatrixEntry, MatrixEntry> {

    private MatrixEntry outputKey = new MatrixEntry();
    private MatrixEntry outputValue = new MatrixEntry();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Parse the input matrix line
      String[] tokens = value.toString().split(",");
      String matrixName = tokens[0];
      int row = Integer.parseInt(tokens[1]);
      int col = Integer.parseInt(tokens[2]);
      int element = Integer.parseInt(tokens[3]);

      if (matrixName.equals("matrix_a")) {
        outputKey = new MatrixEntry(matrixName, row, col, element);
        outputValue = new MatrixEntry(matrixName, col, row, element);
        context.write(outputKey, outputValue);
      } else if (matrixName.equals("matrix_b")) {
        outputKey = new MatrixEntry(matrixName, row, col, element);
        outputValue = new MatrixEntry(matrixName, row, col, element);
        context.write(outputKey, outputValue);
      }
    }
  }

  public static class MatrixReducer extends Reducer<MatrixEntry, MatrixEntry, Text, IntWritable> {

    private Text outputKey = new Text();
    private IntWritable outputValue = new IntWritable();

    public void reduce(MatrixEntry key, Iterable<MatrixEntry> values, Context context) throws IOException, InterruptedException {
      String matrixName = key.getMatrixName().toString();
      int row = key.getRow().get();
      int col = key.getCol().get();
      int partialResult = 0;
      List<MatrixEntry> matrixAValues = new ArrayList<>();
      List<MatrixEntry> matrixBValues = new ArrayList<>();

      for (MatrixEntry value : values) {
        if (value.getMatrixName().toString().equals("A")) {
          // Store matrix A values for the current row
          matrixAValues.add(new MatrixEntry(value.getMatrixName().toString(), value.getRow().get(), value.getCol().get(), value.getValue().get()));
        } else if (value.getMatrixName().toString().equals("B")) {
          // Store matrix B values for the current column
          matrixBValues.add(new MatrixEntry(value.getMatrixName().toString(), value.getRow().get(), value.getCol().get(), value.getValue().get()));
        }
      }

      for (MatrixEntry a : matrixAValues) {
        for (MatrixEntry b : matrixBValues) {
          // Perform matrix multiplication
          if (a.getCol().get() == b.getRow().get()) {
            partialResult += a.getValue().get() * b.getValue().get();
          }
        }
      }

      // Emit the final key-value pair with the result
      outputKey.set(matrixName + "," + row + "," + col);
      outputValue.set(partialResult);
      context.write(outputKey, outputValue);
    }
  }

  public static class CompositeKeyPartitioner extends Partitioner<MatrixEntry, MatrixEntry> {

    private HashPartitioner<Text, IntWritable> hashPartitioner = new HashPartitioner<>();

    @Override
    public int getPartition(MatrixEntry key, MatrixEntry value, int numPartitions) {
      // Partition based on the matrix name and row
      return hashPartitioner.getPartition(key.getMatrixName(), value.getValue(), numPartitions);
    }
  }

  public static class CompositeKeyGroupingComparator extends WritableComparator {

    protected CompositeKeyGroupingComparator() {
      super(MatrixEntry.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      MatrixEntry entryA = (MatrixEntry) a;
      MatrixEntry entryB = (MatrixEntry) b;
      // Group by matrix name and row
      return entryA.compareTo(entryB);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Matrix Multiplication");
    job.setJarByClass(MatrixMultiplication.class);
    job.setMapperClass(MatrixMapper.class);
    job.setReducerClass(MatrixReducer.class);
    job.setPartitionerClass(CompositeKeyPartitioner.class);
    job.setGroupingComparatorClass(CompositeKeyGroupingComparator.class);
    job.setMapOutputKeyClass(MatrixEntry.class);
    job.setMapOutputValueClass(MatrixEntry.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
