package com.bigdata.analysisservice.service;

import com.bigdata.analysisapi.entity.vo.rsp.StudentVoRsp;

/**
 * @Description: java类作用描述
 * @Author: 邓忠情
 * @CreateDate: 2019/1/7 16:50
 * @Version: 1.0
 */

public interface StudentService {
    /**
     *
     * @param keyId
     * @return
     */
    public StudentVoRsp findList(int keyId);
}
