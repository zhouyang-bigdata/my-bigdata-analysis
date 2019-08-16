package com.bigdata.hadoop.utils.common.utils;

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class MyClassLoader extends URLClassLoader{  
  
//  @Override  
//  public Class<?> loadClass(String name) throws ClassNotFoundException {  
//      return findClass(name);  
//  }  
      
    public MyClassLoader(URL[] urls) {
		super(urls);
		// TODO Auto-generated constructor stub
	}
    
    public MyClassLoader(URL[] urls, ClassLoader parent){
    	super(urls,parent);
		// TODO Auto-generated constructor stub
    }
    
	@Override  
    public Class<?> findClass(String name) throws ClassNotFoundException {  
		return findClass(name,null);
    }  
  
	public Class<?> findClass(String name,String path) throws ClassNotFoundException {  
		String classPath = path;
		if(classPath == null){
			classPath = MyClassLoader.class.getResource("/").getPath(); //得到classpath  
		}
        String fileName = name.replace(".", "/") + ".class" ;  
        File classFile = new File(classPath , fileName);  
        if(!classFile.exists()){  
            throw new ClassNotFoundException(classFile.getPath() + " 不存在") ;  
        }  
        ByteArrayOutputStream bos = new ByteArrayOutputStream() ;  
        ByteBuffer bf = ByteBuffer.allocate(1024) ;  
        FileInputStream fis = null ;  
        FileChannel fc = null ;  
        try {  
            fis = new FileInputStream(classFile) ;  
            fc = fis.getChannel() ;  
            while(fc.read(bf) > 0){  
                bf.flip() ;  
                bos.write(bf.array(),0 , bf.limit()) ;  
                bf.clear() ;  
            }  
        } catch (FileNotFoundException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }finally{  
            try {  
                fis.close() ;  
                fc.close() ;  
            } catch (IOException e) {  
                e.printStackTrace();  
            }  
        }  
        return defineClass(bos.toByteArray() , 0 , bos.toByteArray().length) ;  
    }  
	
  
  
  
}  