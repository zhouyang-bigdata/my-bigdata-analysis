
package com.bigdata.analysisservice.controller;

import com.bigdata.analysisapi.entity.vo.rsp.StudentVoRsp;
import com.bigdata.analysisservice.service.StudentService;
import com.bigdata.analysisapi.utils.ResultUtils;
import com.bigdata.analysisapi.utils.ResultVo;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;



@RestController
@RequestMapping("radio-api/v1/private/commom/")
@Slf4j
public class TestController {
    @Autowired
    StudentService studentService;

    @ApiOperation(value = "查询车型列表 ", notes = "")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "app对应ID", required = true, defaultValue = "0", dataType = "Integer", paramType = "query"),
            @ApiImplicitParam(name = "pageNum", value = "当前页码 从0开始", required = true, defaultValue = "0", dataType = "Integer", paramType = "query"),
            @ApiImplicitParam(name = "pageSize", value = "每页记录数量", required = true, defaultValue = "10", dataType = "Integer", paramType = "query")})
    @ApiResponse(code = 200, message = "", response = ResultVo.class)
    @GetMapping("radio/categories")
    public ResultVo<StudentVoRsp> updateIcon()
            throws Exception {
        StudentVoRsp studentPo=studentService.findList(1);
        return ResultUtils.success(studentPo);
    }
}