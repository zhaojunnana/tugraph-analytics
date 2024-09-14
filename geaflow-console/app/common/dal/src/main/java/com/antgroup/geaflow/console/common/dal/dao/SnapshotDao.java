/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.antgroup.geaflow.console.common.dal.dao;

import com.antgroup.geaflow.console.common.dal.entity.GeaflowSnapshotEntity;
import com.antgroup.geaflow.console.common.dal.mapper.GeaflowSnapshotMapper;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.common.dal.model.SnapshotSearch;
import com.github.yulichang.wrapper.MPJLambdaWrapper;
import org.springframework.stereotype.Repository;

@Repository
public class SnapshotDao extends SystemLevelDao<GeaflowSnapshotMapper, GeaflowSnapshotEntity>
        implements DataDao<GeaflowSnapshotEntity, SnapshotSearch> {

    @Override
    public PageList<GeaflowSnapshotEntity> search(SnapshotSearch search) {
        MPJLambdaWrapper<GeaflowSnapshotEntity> wrapper = new MPJLambdaWrapper<>();
        if (search.getInstanceId() != null && !search.getInstanceId().isEmpty()) {
            wrapper.eq(GeaflowSnapshotEntity::getInstanceId, search.getInstanceId());
        }
        if (search.getGraphId() != null && !search.getGraphId().isEmpty()) {
            wrapper.eq(GeaflowSnapshotEntity::getGraphId, search.getGraphId());
        }
        return DataDao.super.search(wrapper, search);
    }
}
