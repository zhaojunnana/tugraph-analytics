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

package com.antgroup.geaflow.console.core.service;

import com.antgroup.geaflow.console.common.dal.dao.NameDao;
import com.antgroup.geaflow.console.common.dal.dao.SnapshotDao;
import com.antgroup.geaflow.console.common.dal.entity.GeaflowSnapshotEntity;
import com.antgroup.geaflow.console.common.dal.model.SnapshotSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.core.model.data.GeaflowSnapshot;
import com.antgroup.geaflow.console.core.service.converter.NameConverter;
import com.antgroup.geaflow.console.core.service.converter.SnapshotConverter;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SnapshotService extends NameService<GeaflowSnapshot, GeaflowSnapshotEntity, SnapshotSearch> {

    @Autowired
    private SnapshotDao snapshotDao;

    @Autowired
    private SnapshotConverter snapshotConverter;

    @Override
    protected List<GeaflowSnapshot> parse(List<GeaflowSnapshotEntity> entities) {
        return ListUtil.convert(entities, e -> snapshotConverter.convert(e));
    }

    @Override
    protected NameDao<GeaflowSnapshotEntity, SnapshotSearch> getDao() {
        return snapshotDao;
    }

    @Override
    protected NameConverter<GeaflowSnapshot, GeaflowSnapshotEntity> getConverter() {
        return snapshotConverter;
    }
}
