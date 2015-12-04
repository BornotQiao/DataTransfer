package com.mobivans.remote.ftp;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;


public class FtpUtilsTest {
	
	@Test
	public void testString() throws Exception {
		String str = "E:/logftp/aa/xxxxx/ggg.nnn.log";
		System.out.println(str.substring(str.lastIndexOf("/"), str.length()));
	}
	
	@Test
	public void testListDirs() throws Exception {
		List<String> rootDirs = FtpUtils.listFiles("/");
		for (String dir : rootDirs) {
			System.out.println(dir);
		}
	}
	
	@Test
	public void testGetStream() throws Exception {
		InputStream stream = FtpUtils.getFileStream("20150614/201506141230_123.56.155.33.log.gz");
		System.out.println(stream.available());
		stream.close();
	}
	
	@Test
	public void testDownload() throws Exception {
		String remote = "20150614/201506141230_123.56.155.33.log.gz";
		String local = "C:/Users/Administrator/Desktop/";
//		String local = "E:/log/20150613";
		FtpUtils.downloadUnzipFile(remote, local);
	}
	
	@Test
	public void testUtilsDownloadThread() throws Exception {
		String local = "E:/logftp/";
//		FtpUtils.allFoldersDownloader(local);
//		FtpUtils.tenFoldersDownloader("20150701",local,5);
		FtpUtils.restFilesDownloader("20150607/201506072145_123.56.155.33.log.gz", local);
	}
	
	@Test
	public void testRandomAccessFile() throws Exception {
//		String lastLine = FtpUtils.readLastLine(new File("log/done.log"));
//		System.out.println(lastLine);
//		String sss = "ddaa";
//		int len = sss.split("/").length;
//		System.out.println(len);
		
		String curr = "201506072145_123.56.155.33.log.gz";
		List<String> files = FtpUtils.listFiles("20150607");
		int index = files.indexOf(curr);
		System.out.println(files.size() + ":" + index);
		
		List<String> tmp = new ArrayList<String>();
		
		long tar = Long.parseLong(curr.split("_")[0]);
		for (String file : files) {
			if(Long.parseLong(file.split("_")[0]) <= tar){
				tmp.add(file);
			}
		}
		
		boolean ss = files.removeAll(tmp);
		System.out.println(ss + "::::" + files.size());
	}
	
}
