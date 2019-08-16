package com.bigdata;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

/*
 * @Author zhouyang
 * @Description //TODO 
 * @Date 17:58 2019/6/6
 * @Param 
 * @return  
 **/

@SpringBootApplication
@EnableScheduling
@EnableAsync
@EnableTransactionManagement
@ComponentScan(basePackages = {"com.bigdata"})
@MapperScan(basePackages = {"com.bigdata.**.dao"})
public class AppApplication extends WebMvcConfigurerAdapter {

    public static void main(String[] args) {
        SpringApplication.run(AppApplication.class, args);
    }
//
//    @Override
//    public void addInterceptors(InterceptorRegistry registry) {
//        registry.addInterceptor(oauth2Interceptor());
//    }
//
//    @Bean
//    public Oauth2Interceptor oauth2Interceptor() {
//        return new Oauth2Interceptor();
//    }

}
