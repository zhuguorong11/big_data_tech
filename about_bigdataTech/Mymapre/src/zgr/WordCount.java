package zgr;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {// 要求实现单词统计的处理操作
	// 在整个代码中最为关键的部分就是Map部分与Reduce部分，都要用户自己来实现
	/*
	 * 进行map的数据处理Object表示输入数据的具体内容，Text每行的文本数据Text每个单词分解后的统计结果IntWritable输出记录结果
	 * 
	 * 
	 * IntWritable相当于java里的Integer类型，Text相当于java里的String类型
	 * 
	 * Context被称为上下文机制
	 */
	private static class WordCountMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		@Override
		protected void map(Object key, Text value,
				Context context)
				throws IOException, InterruptedException {
			// 默认情况下是取每行的数据，所以每行的数据里面都会存在空格，那么要按照空格进行拆分，每当出现一个单词就需要做一个统计的1
			String lineContent = value.toString();// 每行的数据
			String[] result = lineContent.split(" ");// 按照空格进行数据拆分
			for (int x = 0; x < result.length; x++) {
				context.write(new Text(result[x]), new IntWritable(1));// 每一单词最终生成的保存个数是1
			}
		}

	}

	/*
	 * 进行合并后数据的最终统计 Text 表示map输出的文本内容 IntWritable,map处理的个数 Text,Ruducer输出文本
	 * IntWritable，Reducer的输出个数
	 */
	private static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			int sum = 0;// 保存每个单词出现的数据量
			for (IntWritable count : values) {
				sum += count.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "F:\\MyDownloads\\Download\\hadoop-2.7.3");
//		if (args.length != 2) {
//			System.out
//					.println("本程序需要来2个参数，执行：hadoop jar yootk.jar /input/README.txt  /output");
//			System.exit(1);
//		}
		// 每一次的执行实际上都属于一个作业Job，但是希望可以通过初始化参数来设置HDFS的文件存储的路径。
		// 假设现在的文件要保存在HDFS上的“input/README.txt”上，而且输出结果也将保存在HDFS的“/output”目录中
		Configuration conf = new Configuration();

		conf.set("mapred.job.tracker", "10.149.252.106:9001");
		// 考虑到最终需要使用HDFS进行内容的处理操作，并且输入的时候不带有HDFS地址
		String[] argArray = new GenericOptionsParser(conf, args)
				.getRemainingArgs();// 对输入的参数进行处理
		// 后面就需要通过作业进行处理了，而且Map和Reducer操作必须通过作业来进行配置
		Job job = Job.getInstance(conf, "hadoop");// 定义一个hadoop作业
		job.setJarByClass(WordCount.class);// 设置执行的jar文件的程序类
		job.setMapperClass(WordCountMapper.class);// 指定Mapper的处理类
		job.setMapOutputKeyClass(Text.class);// 设置输出的key的类型
		job.setMapOutputValueClass(IntWritable.class);// 设置输出的vaue的类型

		job.setReducerClass(WordCountReducer.class);// 设置reducer操作的处理类
		// 随后需要设置Map-Reduce最终的执行结果
		job.setOutputKeyClass(Text.class);// 信息设置为文本
		job.setOutputValueClass(IntWritable.class);// 最终内容的设置为一个数值
		// 设置输入和输出路径。
		FileInputFormat.addInputPath(job, new Path("hdfs://10.149.252.106:9000/input/NOTICE.txt"));
		;// 设置输入路径
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.149.252.106:9000/output/wordcount/"));// 设置输出路径
		// 等待执行完毕
		System.exit(job.waitForCompletion(true) ? 0 : 1);// 执行若是true,则执行完退出
	}
}
