
  
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Averageage {
	
	// Job1 To find the minimum age of direct friends
// First mapper class
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text val, Context con) throws IOException, InterruptedException {
			String[] friends = val.toString().split("\t");
			if (friends.length == 2) {
				con.write(new Text(friends[0]), new Text(friends[1]));
			}
		}
	}
// first reducer
	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
// method calculate age
		private int calculateAge(String str) throws ParseException {
			// get today's date
			Calendar today = Calendar.getInstance();
			// format input date
			SimpleDateFormat sdformat = new SimpleDateFormat("MM/dd/yyyy");
			Date date = sdformat.parse(str);
			Calendar dob = Calendar.getInstance();
			// set time of dob
			dob.setTime(date);
			// get present year
			int currentYear = today.get(Calendar.YEAR);
			//year of birth
			int yearofbirth = dob.get(Calendar.YEAR);
			// find age by subtracting today's year and dob year
			int age = currentYear - yearofbirth;
			// Check for month if passed or not
			int currentMonth = today.get(Calendar.MONTH);
			int mthofbirth = dob.get(Calendar.MONTH);
			// if month of birth has not arrived means you have not turned the age you should and subtract age
			if (mthofbirth > currentMonth) { // this year can't be counted!
				age--;
			}// if current month but date of birth has nnot arrived then also age has to be subtracted
			else if (mthofbirth == currentMonth) { // same month? check for day
				int currentDay = today.get(Calendar.DAY_OF_MONTH);
				int dobDay = dob.get(Calendar.DAY_OF_MONTH);
				if (dobDay > currentDay) { // this year can't be counted!
					age--;
				}
			}
			return age;
		}

		static HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();

		// setup
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path part = new Path(conf.get("Data"));// Location of file in HDFS
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus status : fss) {
				Path pt = status.getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String record;
				record = br.readLine();
				while (record != null) {
					String[] data = record.split(",");
					if (data.length == 10) {
						try {
							int age = calculateAge(data[9]);
							map.put(Integer.parseInt(data[0]), age);
						} catch (ParseException e) {
							e.printStackTrace();
						}
					}
					record = br.readLine();
				}
			}
		}
		// reduce

		public void reduce(Text key, Iterable<Text> val, Context con) throws IOException, InterruptedException {
			for (Text set : val) {
				String[] friendList = set.toString().split(",");
				int minAge = 1000;
				int age;
				for (String perFriend : friendList) {
					age = map.get(Integer.parseInt(perFriend));
					if (age < minAge) {
						minAge = age;
					}
				}
				con.write(key, new Text(Integer.toString(minAge)));
			}
		}
	}

	//Job2 arrange the data by decreasing values of minimum age
	// Second mapper class to perform job 2
	public static class Map2 extends Mapper<LongWritable, Text, LongWritable, Text> {
		private LongWritable count = new LongWritable();

		public void map(LongWritable key, Text val, Context con) throws IOException, InterruptedException {
			String[] data = val.toString().split("\t");
			if (data.length == 2) {
				count.set(Long.parseLong(data[1]));
				con.write(count, new Text(data[0]));
			}
		}
	}
	// Second reducer
	public static class Reducer2 extends Reducer<LongWritable, Text, Text, Text> {
		public void reduce(LongWritable key, Iterable<Text> val, Context con)
				throws IOException, InterruptedException {
			for (Text set : val) {
				con.write(set, new Text(Long.toString(key.get())));
			}
		}
	}
	
	//Job3 Only top 10 values 
// Third mapper class to perform job 3
	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
		private int count = 0;

		public void map(LongWritable key, Text val, Context con) throws IOException, InterruptedException {
			if (count < 10) {
				String[] data = val.toString().split("\t");
				if (data.length == 2) {
					count++;
					con.write(new Text(data[0]), new Text(data[1]));
				}
			}
		}
	}
	// Third reducer
	public static class Reducer3 extends Reducer<Text, Text, Text, Text> {
		static HashMap<Integer, String> map = new HashMap<Integer, String>();
// setup method
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path part = new Path(conf.get("Data"));// Location of file in HDFS
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus status : fss) {
				Path pt = status.getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String record;
				record = br.readLine();
				while (record != null) {
					String[] data = record.split(",");
					if (data.length == 10) {
						map.put(Integer.parseInt(data[0]), data[1] + "," + data[2] + "," + data[3] + "," + data[4] + "," + data[5]);
					}
					record = br.readLine();
				}
			}
		}
		
		public void reduce(Text key, Iterable<Text> val, Context con) throws IOException, InterruptedException {
			int userId = Integer.parseInt(key.toString());
			String userData = map.get(userId);
			for (Text set : val) {
				con.write(new Text(userData), set);
			}
		}
	}
	
	
	// Main function to set up configuration jobs to call all the mapper and reducer classes
	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		// there should be 5 arguments
		if (otherArgs.length != 5) {
			System.err.println("Usage: <File with user and friends> <userdata.txt> <tempPath_job1> <tempPath_job2><output_Path>");
			System.exit(2);
		}
		conf1.set("Data", otherArgs[1]);
		Job job1 = Job.getInstance(conf1, "Minimum Age");
		// Job setup for first job and mapper reducer classes are called
		job1.setJarByClass(Averageage.class);
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reducer1.class);

		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}
// Job setup for second job after successful completion of first job and mapper reducer classes are called
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Decreasing order");

		job2.setJarByClass(Averageage.class);
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reducer2.class);

		FileInputFormat.addInputPath(job2, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));

		job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}
		/// Job setup for third job after successful completion of second job and mapper reducer classes are called
		Configuration conf3 = new Configuration();
		conf3.set("Data", otherArgs[1]);
		Job job3 = Job.getInstance(conf3, "Decreasing order");

		job3.setJarByClass(Averageage.class);
		job3.setMapperClass(Map3.class);
		job3.setReducerClass(Reducer3.class);

		FileInputFormat.addInputPath(job3, new Path(otherArgs[3]));
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[4]));

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		if (!job3.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}
