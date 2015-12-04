package com.mobivans.remote.main;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;

import com.mobivans.remote.ftp.FtpUtils;

public class Main {
	
	private static Logger logger = Logger.getLogger(Main.class);
	
	/**
	 * main
	 * @param args
	 */
	public static void main(String[] args) {
		startDownload("E:/logftp/");
	}
	
	public static void startUpload(){
		
	}
	
	/**
	 * 下载到本地，断点续下
	 * @param local
	 */
	public static void startDownload(String local){
		File logfile = new File("log/done.log");
		//判断记录文件是否存在
		if(logfile.length() != 0){
			//最后一个下载成功的文件记录
			String lastLine = FtpUtils.readLastLine(logfile);
			//最后一个下载成功的文件
			String remoteFile = lastLine.split(" ")[2];
			String beginDir = "";
			List<String> files = FtpUtils.listFiles(remoteFile.split("/")[0]);
			//判断当前文件是否是该文件夹最后一个文件
			if((files.size()-1) != files.indexOf(remoteFile.split("/")[1])){//不是该文件夹下最后一个文件
				//继续下载没下完的文件的文件夹
				FtpUtils.restFilesDownloader(remoteFile, local);
			}
			//下载后续的文件夹
			beginDir = Long.parseLong(remoteFile.split("/")[0])+1+"";
			FtpUtils.tenFoldersDownloader(beginDir, local, 7);
		} else{
			//记录日志不存在，就从第一个开始下载
			FtpUtils.tenFoldersDownloader(FtpUtils.listFiles("").get(0) , local, 7);
		}
	}
}
