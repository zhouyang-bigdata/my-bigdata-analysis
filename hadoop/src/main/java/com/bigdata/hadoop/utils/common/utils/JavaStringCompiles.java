package com.bigdata.hadoop.utils.common.utils;

import org.apache.log4j.Logger;

import javax.tools.*;
import javax.tools.Diagnostic.Kind;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JavaStringCompiles {
	Logger log = Logger.getLogger(getClass());

	// 根路径
	private String path;
	private String outputPath;
	// 编译结果信息
	private List<String> compilesMessage = new ArrayList<String>();

	public List<String> getCompilesMessage() {
		return compilesMessage;
	}

	public void setCompilesMessage(List<String> compilesMessage) {
		this.compilesMessage = compilesMessage;
	}

	/**
	 * 编译java代码为class byte[]数据
	 * 
	 * @param className
	 *            类名
	 * @param javaContent
	 *            java代码
	 * @param path
	 * 				class文件存放位置
	 * @return class byte[]数据
	 * @throws CommonException
	 */
	public boolean makeJavaCode(String className, String javaContent,String outputPath)
			throws Exception {
		this.outputPath = outputPath;
		checkPackage();
		return this.makeJavaFile(className, javaContent);
		/*if (!this.makeJavaFile(className, javaContent)) {
			return null;
		}
		return this.getJavaClassData(className);*/
	}

	/**
	 * class文件存放位置
	 */
	private void checkPackage() {
		try {
			this.path = this.getClass().getResource("/").toURI().getPath();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * java 内存动态编译
	 * 
	 * @return true :编译成功；false :编译失败
	 * @throws Exception
	 */
	protected boolean makeJavaFile(String className, String javaContent)
			throws Exception {
		try {
			JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();

			StandardJavaFileManager fileManager = javaCompiler
					.getStandardFileManager(null, null, null);

			JavaFileObject jfo = new JavaSourceFromString(className,
					javaContent);

			// 编译内存文件
			Iterable<JavaFileObject> compilationUnits = Arrays.asList(jfo);

			Iterable<String> options = Arrays.asList("-d", this.outputPath, "-cp",
					this.path);
			// 保存编译结果
			DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
			// 编译内存文件
			javaCompiler.getTask(null, fileManager, diagnostics, options, null,
					compilationUnits).call();
			if (diagnostics.getDiagnostics().size() == 0) {
				return true;
			} else {
				/*
				 * 封装编译出错的信息
				 */
				for (@SuppressWarnings("rawtypes")
				Diagnostic diagnostic : diagnostics.getDiagnostics()) {
					String message = diagnostic.getMessage(null);
					/*message = message.substring(message.lastIndexOf("/") + 1,
							message.length());*/
					String error = "";
					if(diagnostic.getKind() == Kind.ERROR){
						error += "("+(diagnostic.getLineNumber() - 1)+","+diagnostic.getColumnNumber()+"):";
						error += message;
						this.compilesMessage.add(error);
						log.error(error);
					}
				}
				return false;
			}
		} catch (Exception e) {
			log.error("编译出错：" + e);
			throw new Exception(e);
		}
	}
}
