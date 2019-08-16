package com.bigdata.hadoop.utils.hadoopUtils.compress;


import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;

/**
 * @ClassName GzipCompress
 * @Description TODO
 * @Author zhouyang
 * @Date 2019/4/23 11:34
 * @Version 1.0
 **/
public class GzipCompress {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        InputStream in = null;
        OutputStream out = null;
        try {
            FileSystem fs = FileSystem.get(conf);
            // Input file from local file system
            in = new BufferedInputStream(new FileInputStream("/home/knpcode/Documents/knpcode/Hadoop/Test/data.txt"));
            //Compressed Output file
            Path outFile = new Path("/user/compout/test.gz");
            // Verification
            if (fs.exists(outFile)) {
                System.out.println("Output file already exists");
                throw new IOException("Output file already exists");
            }

            out = fs.create(outFile);

            // For gzip compression
            CompressionCodecFactory    factory    = new CompressionCodecFactory(conf);
            CompressionCodec codec = factory.getCodecByClassName("org.apache.hadoop.io.compress.GzipCodec");
            CompressionOutputStream    compressionOutputStream    = codec.createOutputStream(out);

            try {
                IOUtils.copyBytes(in, compressionOutputStream, 4096, false);
                compressionOutputStream.finish();

            } finally {
                IOUtils.closeStream(in);
                IOUtils.closeStream(compressionOutputStream);
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
