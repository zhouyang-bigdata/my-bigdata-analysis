package com.bigdata.analysisservice.entity.po;

import io.swagger.annotations.ApiModelProperty;

public class StudentPo {
    /**
     *
     */
    @ApiModelProperty(value ="")
    private Integer id;

    /**
     *
     */
    @ApiModelProperty(value ="")
    private String name;

    /**
     *
     */
    @ApiModelProperty(value ="")
    private String sex;

    /**
     *
     * @return id
     */
    public Integer getId() {
        return id;
    }

    /**
     *
     * @param id
     */
    public void setId(Integer id) {
        this.id = id;
    }

    /**
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     *
     * @param name
     */
    public void setName(String name) {
        this.name = name == null ? null : name.trim();
    }

    /**
     *
     * @return sex
     */
    public String getSex() {
        return sex;
    }

    /**
     *
     * @param sex
     */
    public void setSex(String sex) {
        this.sex = sex == null ? null : sex.trim();
    }
}