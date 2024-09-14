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

import com.antgroup.geaflow.console.common.dal.dao.DataDao;
import com.antgroup.geaflow.console.common.dal.dao.SnapshotDao;
import com.antgroup.geaflow.console.common.dal.entity.GeaflowSnapshotEntity;
import com.antgroup.geaflow.console.common.dal.model.SnapshotSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.data.GeaflowSnapshot;
import com.antgroup.geaflow.console.core.service.converter.DataConverter;
import com.antgroup.geaflow.console.core.service.converter.SnapshotConverter;
import com.antgroup.geaflow.console.core.service.store.GeaflowDataStore;
import com.antgroup.geaflow.console.core.service.store.factory.DataStoreFactory;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SnapshotService extends DataService<GeaflowSnapshot, GeaflowSnapshotEntity, SnapshotSearch> {

    @Autowired
    private SnapshotDao snapshotDao;

    @Autowired
    private SnapshotConverter snapshotConverter;

    @Autowired
    private DataStoreFactory dataStoreFactory;

    @Autowired
    private PluginService pluginService;

    @Override
    protected List<GeaflowSnapshot> parse(List<GeaflowSnapshotEntity> entities) {
        return ListUtil.convert(entities, e -> snapshotConverter.convertEntity(e));
    }

    @Override
    protected DataDao<GeaflowSnapshotEntity, SnapshotSearch> getDao() {
        return snapshotDao;
    }

    @Override
    protected DataConverter<GeaflowSnapshot, GeaflowSnapshotEntity> getConverter() {
        return snapshotConverter;
    }

    public boolean snapshot(GeaflowGraph graph, GeaflowSnapshot geaflowSnapshot) {
        GeaflowPluginCategory category = GeaflowPluginCategory.DATA;
        String dataType = pluginService.getDefaultPlugin(category).getType();
        GeaflowDataStore dataStore = dataStoreFactory.getDataStore(dataType);
        dataStore.snapshotGraphDate(graph, geaflowSnapshot);
        return true;
    }

    public GeaflowSnapshot getByNameInstanceIdAndGraphId(String instanceId, String graphId, String snapshotName) {
        GeaflowSnapshotEntity geaflowSnapshotEntity = snapshotDao.lambdaQuery()
                .eq(GeaflowSnapshotEntity::getInstanceId, instanceId)
                .eq(GeaflowSnapshotEntity::getGraphId, graphId)
                .eq(GeaflowSnapshotEntity::getName, snapshotName)
                .one();
        return parse(geaflowSnapshotEntity);
    }

    public boolean deleteData(GeaflowGraph graph, GeaflowSnapshot geaflowSnapshot) {
        GeaflowPluginCategory category = GeaflowPluginCategory.DATA;
        String dataType = pluginService.getDefaultPlugin(category).getType();
        GeaflowDataStore dataStore = dataStoreFactory.getDataStore(dataType);
        return dataStore.deleteSnapshotData(graph, geaflowSnapshot);
    }

}
