//package com.mobivans.remote.ftp;
//
//import com.mobivans.remote.ftp.FtpUtils.FTP;
//
//public class DownloadThread extends Thread {
//	// FTP
//	FTP ftp;
//	// 远程文件
//	String remote;
//	// 本地目录
//	String local;
//	// test
//	String test;
//	
//	public DownloadThread(String test) {
//		super();
//		this.test = test;
//	}
//
//	public DownloadThread(FTP ftp, String remote, String local) {
//		super();
//		this.ftp = ftp;
//		this.remote = remote;
//		this.local = local;
//	}
//	
//	public void run() {
//		//下载文件
//		FtpUtils.downloadUnzipFile(ftp, remote, local);
//		//下载文件夹
////		FtpUtils.downloadDir(ftp, remote, local);
//		//test
////		System.out.println(Thread.currentThread().getName() + " : " +test);
//	}
//
//}
