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

package com.antgroup.geaflow.console.biz.shared.impl;

import com.antgroup.geaflow.console.biz.shared.GraphSnapShotManager;
import com.antgroup.geaflow.console.biz.shared.convert.DataViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.SnapshotViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.GraphSnapshotView;
import com.antgroup.geaflow.console.common.dal.entity.GeaflowSnapshotEntity;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.common.dal.model.SnapshotSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.data.GeaflowSnapshot;
import com.antgroup.geaflow.console.core.service.DataService;
import com.antgroup.geaflow.console.core.service.GraphService;
import com.antgroup.geaflow.console.core.service.SnapshotService;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class GraphSnapshotManagerImpl
        extends DataManagerImpl<GeaflowSnapshot, GraphSnapshotView, SnapshotSearch>
        implements GraphSnapShotManager {

    @Autowired
    private SnapshotService snapshotService;

    @Autowired
    private GraphService graphService;

    @Autowired
    private SnapshotViewConverter snapshotViewConverter;

    @Override
    public PageList<GraphSnapshotView> search(SnapshotSearch search) {
        String instanceId = getInstanceIdByName(search.getInstanceName());
        search.setInstanceId(instanceId);
        GeaflowGraph graph = graphService.getByName(instanceId, search.getGraphName());
        if (graph == null) {
            if (search.getGraphName() != null) {
                search.setGraphId("-1");
            }
        } else {
            search.setGraphId(graph.getId());
        }
        return super.search(search);
    }

    @Override
    public boolean snapshot(String instanceName, String graphName, GeaflowSnapshot geaflowSnapshot) {
        String instanceId = getInstanceIdByName(instanceName);
        geaflowSnapshot.setInstanceId(instanceId);
        GeaflowGraph graph = graphService.getByName(instanceId, graphName);
        geaflowSnapshot.setGraphId(graph.getId());
        return snapshotService.snapshot(graph, geaflowSnapshot);
    }

    @Override
    public boolean delete(String instanceName, String graphName, String snapshotName) {
        String instanceId = getInstanceIdByName(instanceName);
        GeaflowGraph graph = graphService.getByName(instanceId, graphName);
        GeaflowSnapshot snapshot = snapshotService.getByNameInstanceIdAndGraphId(instanceId,
                graph.getId(), snapshotName);
        if (snapshotService.deleteData(graph, snapshot)) {
            return snapshotService.drop(snapshot.getId());
        }
        return false;
    }

    @Override
    public DataService<GeaflowSnapshot, GeaflowSnapshotEntity, SnapshotSearch> getService() {
        return snapshotService;
    }

    @Override
    protected DataViewConverter<GeaflowSnapshot, GraphSnapshotView> getConverter() {
        return snapshotViewConverter;
    }

    @Override
    protected List<GeaflowSnapshot> parse(List<GraphSnapshotView> views) {
        return ListUtil.convert(views, snapshot -> {
            return snapshotViewConverter.convertView(snapshot);
        });
    }
}
