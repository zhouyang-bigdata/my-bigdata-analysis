package com.bigdata.analysisservice.service.impl;

import com.bigdata.analysisservice.dao.StudentPoMapper;
import com.bigdata.analysisapi.entity.vo.rsp.StudentVoRsp;
import com.bigdata.analysisservice.mapstruct.StudentMapStruct;
import com.bigdata.analysisservice.service.StudentService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/*
java类作用描述
 */
@Service
public class StudentServiceImpl implements StudentService {
    @Autowired
    private StudentPoMapper studentPoMapper;
    @Autowired
    StudentMapStruct studentMapStruct;

    @Override
    public StudentVoRsp findList(int keyId) {
        return studentMapStruct.fromEntity2Vo(studentPoMapper.selectByPrimaryKey(keyId)) ;
    }
}
