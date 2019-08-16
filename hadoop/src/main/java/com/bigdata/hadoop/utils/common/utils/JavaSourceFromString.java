package com.bigdata.hadoop.utils.common.utils;

import javax.tools.SimpleJavaFileObject;
import java.net.URI;

public class JavaSourceFromString extends SimpleJavaFileObject{
	
	// 源码    
    final String code; 
	
    /**
     * 从字符串中构造一个FileObject
     * 
     * @param uri
     * 		此文件对象的 URI
     * @param kind
     * 		此文件对象的种类
     */
	public JavaSourceFromString(String uri, String kind) {     
        super(URI.create("string:///" + uri.replace('.','/') + Kind.SOURCE.extension), Kind.SOURCE);     
        this.code = kind;     
    }     
    
    @Override    
    public CharSequence getCharContent(boolean ignoreEncodingErrors) {     
        return code;     
    }     
}
