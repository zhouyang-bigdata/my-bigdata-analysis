package com.bigdata.hadoop.utils.common.utils;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;

/**
 * 执行脚本
 * Created by cyh on 2017/12/22.
 */
public class ScriptExcutor {
	public static Object excute(String classPath,String className,String methodName, Class<?>[] clazz,Object[] params) throws Exception{
		String message = "";
		File file=new File(classPath);//类路径(包文件上一层)
		URL url=file.toURI().toURL();
		// 通过class URL 拿到classLoader
		MyClassLoader urlc = new MyClassLoader(new URL[] { url },
				Thread.currentThread().getContextClassLoader());//Thread.currentThread().getContextClassLoader() 不加会导致参数类型不匹配的异常
		Class<?> cls=urlc.findClass(className,classPath);//加载指定类，注意一定要带上类的包名
		Object obj=cls.newInstance();//初始化一个实例
		Method method;
		Object result;
		if(clazz == null && params == null){
			method=cls.getMethod(methodName);//方法名和对应的参数类型
			result = method.invoke(obj);//调用得到的上边的方法method
		}else{
			method=cls.getMethod(methodName,clazz);//方法名和对应的参数类型
			result = method.invoke(obj,params);//调用得到的上边的方法method
		}
		return result;
	}
}
