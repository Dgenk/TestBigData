package mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CactusPlot {

	/*
	 * classe MyComparable usata per comparare due valori di 'solver','Real' (time)
	 * 
	 */
	public static class MyComparable implements WritableComparable<MyComparable> {

		private String solver;
		private String time;

		public MyComparable() {
		}

		public MyComparable(String solver, String time) {
			super();
			this.solver = solver;
			this.time = time;
		}

		@Override
		public void readFields(DataInput dataInput) throws IOException {
			solver = WritableUtils.readString(dataInput);
			time = WritableUtils.readString(dataInput);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, solver);
			WritableUtils.writeString(out, time);
		}

		@Override
		public int compareTo(MyComparable o) {
			int cmp = solver.compareTo(o.solver);
			double a = Double.parseDouble(time);
			double b = Double.parseDouble(o.time);
			if (cmp == 0) {
				if (a > b)
					return 1;
				else if (a < b)
					return -1;
				else
					return 0;
			}
			return cmp;
		}

		@Override
		public String toString() {
			return String.format("%s:%s", solver, time);
		}

		public String getSolver() {
			return solver;
		}

		public void setSolver(String solver) {
			this.solver = solver;
		}

		public String getTime() {
			return time;
		}

		public void setTime(String time) {
			this.time = time;
		}

	}

	/**
	 * 
	 * SolverGroupComparator raggruppa i valori
	 *
	 */
	public static class SolverGroupComparator extends WritableComparator {

		protected SolverGroupComparator() {
			super(MyComparable.class, true);
		}

		@Override
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			MyComparable ac = (MyComparable) a;
			MyComparable bc = (MyComparable) b;
			if (ac.solver.equals(bc.solver))
				return 0;
			return 1;
		}

	}

	static class CactusMapper1 extends Mapper<LongWritable, Text, MyComparable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, MyComparable, Text>.Context context)
				throws IOException, InterruptedException {

			String[] row = value.toString().split("\t");// split delle linee per tab

			if (row[14].contains("solved"))// se il valore della colonna result è solved
											// prendo la linea
			{
				context.write(new MyComparable(row[0], row[11]), new Text(row[11]));// nella funzione write del context
																					// gli
				// passo come chiave l'oggetto
				// MyComparable contenente la coppia
				// (key,value)
				// e come value passo un oggetto Text con il valore
			}
		}
	}

	/*
	 * nel reducer separo i valori di ogni singolo Solver per tab
	 */
	static class CactusReducer1 extends Reducer<MyComparable, Text, Text, Text> {
		@Override
		protected void reduce(MyComparable key, Iterable<Text> values,
				Reducer<MyComparable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

			String out = "";
			for (Text text : values) {
				out += text.toString() + "\t";
			}
			context.write(new Text(key.solver), new Text(out));
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job1");

		job.setMapperClass(CactusMapper1.class);
		job.setReducerClass(CactusReducer1.class);
		job.setGroupingComparatorClass(SolverGroupComparator.class);

		job.setOutputKeyClass(MyComparable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
