package com.mobivans.remote.ftp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;

import com.mobivans.remote.hdfs.HdfsUtils;

public class FtpUtils{
	
	private static String host;
	private static int port;
	private static String user;
	private static String pswd;
//	private static String mode;
	
	private static Logger logger = Logger.getLogger(FtpUtils.class);
	private static Logger logger_sucs = Logger.getLogger("success_logger");
	private static Logger logger_fail = Logger.getLogger("failure_logger");
	
	static {
		//ftp连接信息
		Properties prop = new Properties();
		//读取ftp配置文件
		InputStream stream = FtpUtils.class.getResourceAsStream("/com/mobivans/config/ftpconf.properties");
		try {
			//加载配置文件
			prop.load(stream);
			host = prop.getProperty("datavans.ftp.host");
			port = Integer.parseInt(prop.getProperty("datavans.ftp.port"));
			user = prop.getProperty("datavans.ftp.user");
			pswd = prop.getProperty("datavans.ftp.pswd");
			//关闭流
			stream.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	/**
	 * 获取ftp链接
	 * @return ftpClient 对象
	 */
	private static FTPClient getConnect(){
		FTPClient client = new FTPClient();
		try {
			// 连接FTP服务器
			client.connect(host, port);
			// 登录FTP服务器
			client.login(user, pswd);
			
			//判断是否为有效连接
			if(!FTPReply.isPositiveCompletion(client.getReplyCode())){
				client.disconnect();
				logger.error("FTP connection is invalid!~");
				return null;
			}
			//设置被动模式
			client.enterLocalPassiveMode();
			//更改ftp操作目录，获取连接后，默认是根目录
			client.changeWorkingDirectory("/");
			
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return client;
	}
	
	/**
	 * 关闭ftp链接
	 * @param client
	 */
	private static void disConnect(FTPClient client){
		if (client.isConnected()) {
			try {
				//千万不能logout，否则程序会卡在这里，没办法断开连接
//				client.logout();
				client.disconnect();
			} catch (IOException e) {
				e.printStackTrace();
			} 
		}
	}
	
	/**
	 * 列出目录下所有文件（夹）
	 * @return 返回根目录下的所有文件或者文件夹
	 */
	public static List<String> listFiles(String path){
		List<String> dirs = new ArrayList<String>();
		// 获取连接
		FTPClient ftpClient = getConnect();
		try {
			//判断给定目录是否空串来更改操作目录
			if(!"".equals(path))
				ftpClient.changeWorkingDirectory(path);
			//列出目录下 所有文件
			FTPFile[] files = ftpClient.listFiles();
			for (FTPFile file : files) {
				String name = file.getName();
				dirs.add(name);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			disConnect(ftpClient);
		}
		return dirs;
	}
	
	/**
	 * 获取根目录下所有日期文件的数字，用于比较
	 * @param path
	 * @return
	 */
	public static List<Long> listFilesDate(){
		List<Long> dirs = new ArrayList<Long>();
		// 获取连接
		FTPClient ftpClient = getConnect();
		try {
			//列出目录下 所有文件
			FTPFile[] files = ftpClient.listFiles();
			for (FTPFile file : files) {
				String name = file.getName();
				//将文件名转换为long
				dirs.add(Long.parseLong(name));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			disConnect(ftpClient);
		}
		return dirs;
	}
	
	/**
	 * 获取FTP文件流
	 * @param remoteFile
	 * @return
	 */
	public static InputStream getFileStream(String remoteFile){
		InputStream stream = null;
		//获取ftp服务器连接
		FTPClient ftpClient = getConnect();
		try {
			logger.info("Downloading file: "+remoteFile +".");
			stream = ftpClient.retrieveFileStream(remoteFile);
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			//关闭ftp连接
			disConnect(ftpClient);
		}
		return stream;
	}
	
	/**
	 * 下载指定FTP文件到指定本地目录
	 * @param remote 远程FTP文件
	 * @param local 本地目录
	 */
	public static void downloadUnzipFile(String remoteFile, String local){
		//获取ftp服务器连接
		FTPClient ftpClient = getConnect();
		//处理remote的文件名
		String[] split = remoteFile.split("/");
		String fileName = split[split.length-1];
		try {
			//判断本地目录是否存在，不存在就创建
			File localDir = new File(local);//local
			if(!localDir.exists()){
				localDir.mkdirs();
			}
			//记录日志
			logger.info("Downloading file <"+remoteFile+">...");
			//判断是否为压缩文件
			if(!remoteFile.endsWith("gz")){
				//不是压缩文件，直接下载
				writeFile(ftpClient.retrieveFileStream(remoteFile), local+fileName);
			} else{
				//是压缩文件，先解压，然后下载
				unZipAndWriteFile(local+"/"+fileName, ftpClient.retrieveFileStream(remoteFile));
			}
			//记录日志
			logger.info("Downloaded file <"+remoteFile+"> successfully.");
			//记录下载成功日志
			logger_sucs.info(remoteFile);
		} catch (IOException e) {
			logger_fail.info(remoteFile);
			logger.error("Write or decompress file <" + fileName + "> is failed.");
		} 
		//关闭ftp连接
		disConnect(ftpClient);
	}
	
	/**
	 * 多线程下载 - 一次下载所有文件夹
	 * @param local
	 */
	public static void allFoldersDownloader(String local){
		ExecutorService executor = Executors.newFixedThreadPool(5);
		//获取根目录下所有文件夹
		List<String> dirs = FtpUtils.listFiles("");
		for (String dir : dirs) {
			//获取根目录下子文件夹下的所有文件-压缩的日志文件
			List<String> files = FtpUtils.listFiles(dir);
			for (String file : files) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				Thread thd = new DownloadThread(dir+"/"+file, local+dir+"/");
				executor.execute(thd);
			}
		}
		executor.shutdown();
	}
	
	/**
	 * 多线程下载 - 一次下载n天
	 * @param beginDir
	 * @param local
	 * @param days 下载n天
	 */
	public static void tenFoldersDownloader(String beginDir, String local, int days){
		ExecutorService executor = Executors.newFixedThreadPool(5);
		//获取根目录下所有文件夹
		List<String> rootDirs = FtpUtils.listFiles("");
		int curr = rootDirs.indexOf(beginDir), tmp = curr;
		//从当前文件夹遍历10个文件夹进行下载
		for(; curr<tmp+days; curr++){
			String dir = rootDirs.get(curr);
			List<String> files = FtpUtils.listFiles(dir);
			for (String file : files) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				Thread thd = new DownloadThread(dir+"/"+file, local+dir+"/");
				executor.execute(thd);
			}
		}
		executor.shutdown();
	}
	
	/**
	 * 下载一个文件夹下剩余没下载完的文件
	 * @param remoteFile
	 * @param local
	 */
	public static void restFilesDownloader(String remoteFile, String local){
		//获取当前文件夹
		String dir = remoteFile.split("/")[0];
		String filename = remoteFile.split("/")[1];
		
		List<String> files = listFiles(dir);
		
		//去除已经下载过的文件
		List<String> tmp = new ArrayList<String>();
		long tar = Long.parseLong(filename.split("_")[0]);
		for (String file : files) {
			if(Long.parseLong(file.split("_")[0]) <= tar){
				tmp.add(file);
			}
		}
		files.removeAll(tmp);
		
		//启动线程池开启后续下载
		ExecutorService executor = Executors.newFixedThreadPool(5);
		for (String file : files) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Thread thd = new DownloadThread(dir+"/"+file, local+dir+"/");
			executor.execute(thd);
		}
		executor.shutdown();
	}
	
	/**
	 * 分割路径文件，获取文件名和扩展名
	 * @param fileName 路径文件
	 * @return map (key: name,ext)
	 */
	public static Map<String, String> splitFileName(String fileName){
		Map<String, String> info = new HashMap<String, String>();
		
		int lastIn = fileName.lastIndexOf(".");
		if(lastIn>0 && lastIn<fileName.length()-1){
			info.put("name", fileName.substring(0, lastIn));
			info.put("ext", fileName.substring(lastIn+1, fileName.length()));
		}
		return info;
	}
	
	/**
	 * 读取文件最后一行
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static String readLastLine(File file) {
		if (!file.exists() || file.isDirectory() || !file.canRead()) {
			return null;
		}
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(file, "r");
			long len = raf.length();
			if (len == 0L) {
				return "";
			} else {
				long pos = len - 1;
				while (pos > 0) {
					pos--;
					raf.seek(pos);
					if (raf.readByte() == '\n') {
						break;
					}
				}
				if (pos == 0) {
					raf.seek(0);
				}
				byte[] bytes = new byte[(int) (len - pos)];
				raf.read(bytes);
				return new String(bytes);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (raf != null) {
				try {
					raf.close();
				} catch (Exception e2) {
				}
			}
		}
		return null;
	}
	
	/**
	 * 将FTP文件流写出到指定文件
	 * @param is
	 * @param dst
	 * @throws IOException
	 */
	private static void writeFile(InputStream is, String dst) throws IOException{
		//读
		BufferedInputStream bis = new BufferedInputStream(is);
		//写
		FileOutputStream fos = new FileOutputStream(new File(dst));
		BufferedOutputStream bos = new BufferedOutputStream(fos);
		
		int read = 0;
		while((read=bis.read()) != -1){
			bos.write(read);
		}
		
		if(bis != null)
			bis.close();
		if(bos != null)
			bos.close();
	}
	
	/**
	 * 解压GZ文件并写出
	 * @param remoteFile
	 * @param in
	 * @throws IOException 
	 * @throws Exception 
	 */
	private static void unZipAndWriteFile(String remoteFile, InputStream in) throws IOException  {
		Map<String, String> info = splitFileName(remoteFile);
		GZIPInputStream gzIn = null;
		FileOutputStream fileOut = null;
		
		gzIn = new GZIPInputStream(in);
		fileOut = new FileOutputStream(info.get("name"));
		
		byte[] buf = new byte[1024*1024];
		int len;
		while((len = gzIn.read(buf)) > 0) {
			fileOut.write(buf, 0, len);
		}
		
		if(gzIn != null)
			gzIn.close();
		if(fileOut != null)
			fileOut.close();
	}
	
	/**
	 * 下载线程类
	 * @author Administrator
	 *
	 */
	public static class DownloadThread extends Thread{
		// 远程文件
		String remote;
		// 本地目录
		String local;
		
		public DownloadThread(String remote, String local) {
			super();
			this.remote = remote;
			this.local = local;
		}
		
		public void run() {
			//test
//			System.out.println(Thread.currentThread().getName() + " : " +test);
			//下载文件
			FtpUtils.downloadUnzipFile(remote, local);
			
			//创建ftp上所有文件夹
//			HdfsUtils.multiCreateFiles();
			
//			HdfsUtils.tenFoldersCreater(beginDir, days);
		}
	}
}