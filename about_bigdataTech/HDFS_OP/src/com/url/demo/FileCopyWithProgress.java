package com.url.demo;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import com.jcraft.jsch.IO;

//将本地文件拷贝至hdfs上	
public class FileCopyWithProgress {
     static {
    	 URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
     }
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
			String src = args[0];
			String dst =args[1];
			Configuration conf = new Configuration();//Configuration对象封装了客户端或者服务器的配置，通过设置配置文件读取类路径来实现如core-site.xml
			FileSystem fs = FileSystem.get(URI.create(dst),conf);//FileSystem是一个通用的文件系统，通过静态方法可以获取
			
			InputStream in = new BufferedInputStream(new FileInputStream(src));//读取本地文件
	
			OutputStream out = fs.create(new Path(dst),new Progressable() {//创建一个输出流至hdfs
					
				@Override
				public void progress() {
					// TODO Auto-generated method stub
					 System.out.print(".");
				}
			});
		         IOUtils.copyBytes(in, out, 4096,false);
		         
	}

}
