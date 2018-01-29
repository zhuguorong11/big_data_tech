package number_tel;
import java.io.IOException;

import javax.tools.Tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.sun.swing.internal.plaf.metal.resources.metal;

public class NumberTel {
   enum Counter
   {
	   LINESKIP,//出错的行数
   }
   private static class  Map extends Mapper<LongWritable, Text, Text, Text>{
		
	 protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			  String line = value.toString();
			  try {
				String[] lineSplit = line.split(" ");//分割
				String anum = lineSplit[0];
				String bnum = lineSplit[1];
				context.write(new Text(bnum), new Text(anum));
			} catch (java.lang.ArrayIndexOutOfBoundsException e) {
				// TODO: handle exception
				context.getCounter(Counter.LINESKIP).increment(1);//出错令计数器+1
			}
		}
		}

	 private static class Reduce extends Reducer<Text, Text, Text, Text>{
 	   
		 protected void reduce(Text key, Iterable<Text> values,Reducer<Text, Text, Text, Text>.Context context)
 					throws IOException, InterruptedException {
 		   	       String valueString;
 		   	       String out = "";
 		   	       for(Text value : values)
 		   	       {
 		   	    	   valueString = value.toString();
 		   	    	   out += valueString + "|";
 		   	       }
 		   	       context.write(key, new Text(out));
    	}
 }
	 public static void main(String[] args)throws Exception{
	    	if(args.length!=2)
	    	{
	    		System.out.println("本程序需要按照参数，执行：hadoop jar yootk.jar /input/README.txt  /output");
	    		System.exit(1);
	    	}
	    	//每一次的执行实际上都属于一个作业Job，但是希望可以通过初始化参数来设置HDFS的文件存储的路径。
	    	//假设现在的文件爱你保存在HDFS上的“input/README.txt”上，而且输出结果也将保存在HDFS的“/output”目录中
	    	Configuration conf = new Configuration();
	    	conf.set("mapred.job.tracker","zgr:9001");
	    	//考虑到最终需要使用HDFS进行内容的处理操作，并且输入的时候不带有HDFS地址
	    	String[] argArray = new GenericOptionsParser(conf,args).getRemainingArgs();//对输入的参数进行处理
	    	//后面就需要通过作业进行处理了，而且Map和Reducer操作必须通过作业来进行配置
	    	Job job = Job.getInstance(conf, "Test1");//定义一个hadoop作业
	    	job.setJarByClass(Test1.class);//设置执行的jar文件的程序类
	    	job.setMapperClass(Map.class);//指定Mapper的处理类
	    	//job.setMapOutputKeyClass(Text.class);//设置输出的key的类型
	    	//job.setMapOutputValueClass(IntWritable.class);//设置输出的value的类型
	    	job.setReducerClass(Reduce.class);//设置reducer操作的处理类
	    	//随后需要设置Map-Reduce最终的执行结果'
	    	job.setOutputFormatClass(TextOutputFormat.class);
	    	job.setOutputKeyClass(Text.class);//信息设置为文本
	    	job.setOutputValueClass(Text.class);//最终内容的设置为一个数值
	    	//设置输入和输出路径。
	    	FileInputFormat.addInputPath(job, new Path(argArray[0]));;//设置输入路径
	    	FileOutputFormat.setOutputPath(job, new Path(argArray[1]));//设置输出路径
	    	//等待执行完毕
	    	System.exit(job.waitForCompletion(true)?0:1);//执行若是true,则执行完退出
	    }

   

}
