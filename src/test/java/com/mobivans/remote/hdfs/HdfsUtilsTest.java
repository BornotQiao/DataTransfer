package com.mobivans.remote.hdfs;


import java.io.InputStream;

import org.junit.Test;

import com.mobivans.remote.ftp.FtpUtils;

public class HdfsUtilsTest {
	
	@Test
	public void testCreate() throws Exception {
		HdfsUtils.createFile("/mobivans/abc.txt","xxxxxxxxxxxxx yyyyyyyyyyyyyyyyyyyyyy iiiiiiiiiiiiiiiiiiiiiiiiiiiii");
	}
	
	@Test
	public void testDelete() throws Exception {
		HdfsUtils.deleteFile("/2222.txt");
	}
	
	@Test
	public void testCopy() throws Exception {
		String localFile = "";
		String hdfsPath = "";
		HdfsUtils.copyFromLocal(true, localFile, hdfsPath);
	}
	
	@Test
	public void testListFiles() throws Exception {
		HdfsUtils.listFiles();
	}
	
	@Test
	public void testCreateFileFromFtp() throws Exception {
//		String dest = "/222";
		String remoteFile = "/20150612/201506120715_123.56.155.33.log.gz";
		
		InputStream ftpFile = FtpUtils.getFileStream(remoteFile);
		HdfsUtils.createFile(remoteFile, ftpFile);
	}
	
	@Test
	public void testMultiUpload() throws Exception {
//		HdfsUtils.multiCreateFiles();
		
		String begin = "20150612";
		HdfsUtils.tenFoldersCreater(begin, 5);
	}
}
