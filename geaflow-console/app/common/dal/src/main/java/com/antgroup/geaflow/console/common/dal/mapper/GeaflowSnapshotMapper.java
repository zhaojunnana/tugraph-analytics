package com.antgroup.geaflow.console.common.dal.mapper;

import com.antgroup.geaflow.console.common.dal.entity.GeaflowSnapshotEntity;
import com.baomidou.mybatisplus.annotation.InterceptorIgnore;
import org.apache.ibatis.annotations.Mapper;

@Mapper
@InterceptorIgnore(tenantLine = "true")
public interface GeaflowSnapshotMapper  extends GeaflowBaseMapper<GeaflowSnapshotEntity> {
}
