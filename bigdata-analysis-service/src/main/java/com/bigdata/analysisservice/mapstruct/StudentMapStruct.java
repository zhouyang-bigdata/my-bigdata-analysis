package com.bigdata.analysisservice.mapstruct;

import com.bigdata.analysisservice.entity.po.StudentPo;

import com.bigdata.analysisapi.entity.vo.rsp.StudentVoRsp;
import org.mapstruct.Mapper;

/**
 * @Description: java类作用描述
 * @Author: 邓忠情
 * @CreateDate: 2019/2/16 11:22
 * @Version: 1.0
 */
@Mapper(componentModel = "spring")
public interface StudentMapStruct {
    StudentVoRsp fromEntity2Vo(StudentPo entity);

//    AppBaseImgInfoPo fromVo2Entity(AppBaseImgInfoPoVo dto);
//
//
//    List<AppBaseImgInfoPoVo>  fromEntity2Vos(List<AppBaseImgInfoPo> entitys);
}
