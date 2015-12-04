package com.mobivans.remote.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.mobivans.remote.ftp.FtpUtils;

public class HdfsUtils {
	
	private static Logger logger = Logger.getLogger(HdfsUtils.class);
	private static Logger logger_sucs = Logger.getLogger("success_logger");
	private static Logger logger_fail = Logger.getLogger("failure_logger");
	
	static{
		System.setProperty("hadoop.home.dir", "E:/hadoop/hadoop-2.6.0");
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	static String localFile = "C:/Users/Administrator/Desktop/test.txt";
//	static String hdfsPath = "hdfs://182.92.216.234:9000/";
	static String hdfsPath = "hdfs://10.19.150.8:9000/";
	
	/**
	 * 获取FileSystem对象实例
	 * @param isRoot 是否需要root权限
	 * @return
	 */
	private static FileSystem getFS(boolean isRoot){
		Configuration conf = new Configuration();
		FileSystem fileSystem = null;
		try {
			if(isRoot)
				fileSystem = FileSystem.get(new URI(hdfsPath), conf, "root");
			else
				fileSystem = FileSystem.get(new URI(hdfsPath), conf);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return fileSystem;
	}
	
	/**
	 * 关闭FileSystem对象实例链接
	 * @param fileSystem
	 */
	private static void closeFS(FileSystem fileSystem){
		try {
			fileSystem.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 创建文件
	 * @param content
	 */
	public static void createFile(String dest, String content){
		FileSystem fs = getFS(true);
		
		FSDataOutputStream fsOutput = null;
		try {
			//此处的path可以是相对路径，也可以是绝对路径
			fsOutput = fs.create(new Path(dest));//new Path(hdfsPath+"/mobi/javaCreate.txt")
			fsOutput.write(content.getBytes());
			
			System.out.println("文件创建成功！");
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fsOutput != null) {
				try {
					fsOutput.close();
				} catch (IOException e) {
					e.printStackTrace();
				} 
			}
			closeFS(fs);
		}
	}
	
	/**
	 * 接收文件流，写出到HDFS
	 * @param path
	 * @param in
	 */
	public static void createFile(String ftpFile, InputStream in){
		//获取连接
		FileSystem fs = getFS(true);
		
		GZIPInputStream gzIn = null;
		FSDataOutputStream fsOutput = null;
		try {
			//此处的path可以是相对路径，也可以是绝对路径
			String hdfsFile = FtpUtils.splitFileName(ftpFile).get("name");
			fsOutput = fs.create(new Path(hdfsFile));
			
			gzIn = new GZIPInputStream(in);
			
			byte[] buf = new byte[1024*1024];
			int len;
			while((len = gzIn.read(buf)) > 0) {
				fsOutput.write(buf, 0, len);
			}
			//记录相关日志
			logger.info("Upload " + hdfsFile + " to HDFS successfully!~");
			//下载成功的记录
			logger_sucs.info(hdfsFile);
		} catch (Exception e) {
			System.out.println(e);
			//记录日志，并将下载失败的文件记录到相关文件
			logger.error("Failed to Upload " + hdfsPath + " to HDFS! Waiting for reupload!");
			//下载失败的记录
			logger_fail.info(hdfsPath);
		} finally {
			if (gzIn != null) {
				try {
					gzIn.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				} 
			}
			if (fsOutput != null) {
				try {
					fsOutput.close();
				} catch (IOException e) {
					e.printStackTrace();
				} 
			}
			closeFS(fs);
		}
	}
	
	/**
	 * 多线程创建文件，创建所有FTP上的文件夹
	 * @param ftp
	 * @param ftpFile
	 * @param dest
	 * @param in
	 */
	public static void multiCreateFiles(){
		ExecutorService executor = Executors.newFixedThreadPool(5);
		//获取ftp根目录下所有文件夹
		List<String> dirs = FtpUtils.listFiles("");
		for (String dir : dirs) {
			//获取ftp根目录下子文件夹下的所有文件-压缩的日志文件
			List<String> files = FtpUtils.listFiles(dir);
			for (String file : files) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				InputStream stream = FtpUtils.getFileStream(dir+"/"+file);
				Thread th = new UploadThread(dir, dir+"/"+file, stream);
				executor.execute(th);
			}
		}
		//关闭线程池
		executor.shutdown();
	}
	
	/**
	 * 多线程创建文件夹，一次创建n个
	 * @param beginDir 开始上传的文件夹
	 * @param days 共n个文件夹
	 */
	public static void tenFoldersCreater(String beginDir, int days){
		ExecutorService executor = Executors.newFixedThreadPool(5);
		//获取根目录所有文件夹
		List<String> rootDirs = FtpUtils.listFiles("");
		//获取当前文件夹在集合中的下标
		int curr = rootDirs.indexOf(beginDir), tmp = curr;
		for(; curr<tmp+days; curr++){
			String dir = rootDirs.get(curr);
			List<String> files = FtpUtils.listFiles(dir);
			for (String file : files) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				InputStream stream = FtpUtils.getFileStream(dir+"/"+file);
				Thread th = new UploadThread(dir, dir+"/"+file, stream);
				executor.execute(th);
			}
		}
		//关闭线程池
		executor.shutdown();
	}
	
	/**
	 * 删除文件
	 * @param path
	 */
	public static boolean deleteFile(String path){
		boolean isDel = false;
		FileSystem fileSystem = getFS(true);
		try {
			//先判断文件是否存在
			if(fileSystem.exists(new Path(path)))
				isDel = fileSystem.delete(new Path(path), true);
			else
				System.err.println("file does not exist!~");
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeFS(fileSystem);
		}
		return isDel;
	}
	
	/**
	 * copyFromLocalFile
	 * @param localFile
	 */
	public static void copyFromLocal(boolean isDele, String localFile, String hdfsPath){
		//判断hdfs路径是否以"/"结尾
		if(!hdfsPath.endsWith("/"))
			hdfsPath = hdfsPath+"/";
		//处理文件名
		String[] split = localFile.split("/");
		String fileName = split[split.length-1];
		
		//获取系统客户端
		FileSystem fs =getFS(true);
		try {
			//支持上传完毕删除源文件，支持多个源文件上传
			fs.copyFromLocalFile(isDele, new Path(localFile), new Path(hdfsPath+fileName));
			
			System.out.println("文件拷贝完成，并删除源文件！");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			closeFS(fs);
		}
	}
	
	/**
	 * 列出文件
	 */
	public static void listFiles() {
		FileSystem fileSystem = getFS(false);
		try {
			FileStatus[] listStatus = fileSystem.listStatus(new Path(hdfsPath));
			for (FileStatus fileStatus : listStatus) {
				boolean isDir = fileStatus.isFile();
				String path = fileStatus.getPath().toString();
				System.out.println(path + "---是文件？>>>" + isDir);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeFS(fileSystem);
		}
	}
	
	/**
	 * 创建线程
	 * @author Administrator
	 *
	 */
	public static class UploadThread extends Thread{
		// 文件上传到hdfs的路径
		String dest;
		// 上传文件名
		String ftpFile;
		// 文件名
		String fileName;
		// ftp文件流
		InputStream in;
		public UploadThread(String dest, String ftpFile, InputStream in) {
			this.dest = dest;
			this.ftpFile = ftpFile;
			this.in = in;
			this.fileName = ftpFile.substring(ftpFile.lastIndexOf("/"), ftpFile.length());
			System.out.println("fileName----> "+fileName);
		}
		
		public void run() {
			//上传到HDFS
//			HdfsUtils.createFile(fileName, in);
			HdfsUtils.createFile(ftpFile, in);
		}
	}
}