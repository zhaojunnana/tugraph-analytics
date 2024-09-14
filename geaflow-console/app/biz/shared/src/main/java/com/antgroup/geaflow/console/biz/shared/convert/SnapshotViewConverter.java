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

package com.antgroup.geaflow.console.biz.shared.convert;

import com.antgroup.geaflow.console.biz.shared.view.GraphSnapshotView;
import com.antgroup.geaflow.console.core.model.data.GeaflowSnapshot;
import org.springframework.stereotype.Component;

@Component
public class SnapshotViewConverter extends DataViewConverter<GeaflowSnapshot, GraphSnapshotView> {
    @Override
    protected GraphSnapshotView modelToView(GeaflowSnapshot model) {
        GraphSnapshotView graphSnapshotView = super.modelToView(model);
        if (model.getTenantId() != null) {
            graphSnapshotView.setTenantId(model.getTenantId());
        }
        graphSnapshotView.setSnapshotTimeLong(model.getSnapshotTimeLong());
        graphSnapshotView.setCheckpoint(model.getCheckpoint());
        graphSnapshotView.setSnapshotPath(model.getSnapshotPath());
        graphSnapshotView.setSnapshotTime(model.getSnapshotTime());
        graphSnapshotView.setInstanceId(model.getInstanceId());
        graphSnapshotView.setStatus(model.getStatus());
        graphSnapshotView.setGraphId(model.getGraphId());
        graphSnapshotView.setSourcePath(model.getSourcePath());
        graphSnapshotView.setFinishTime(model.getFinishTime());
        return graphSnapshotView;
    }

    @Override
    protected GeaflowSnapshot viewToModel(GraphSnapshotView view) {
        GeaflowSnapshot snapshot = super.viewToModel(view);
        snapshot.setSnapshotTimeLong(view.getSnapshotTimeLong());
        snapshot.setCheckpoint(view.getCheckpoint());
        snapshot.setSnapshotPath(view.getSnapshotPath());
        snapshot.setSnapshotTime(view.getSnapshotTime());
        snapshot.setInstanceId(view.getInstanceId());
        snapshot.setStatus(view.getStatus());
        snapshot.setGraphId(view.getGraphId());
        snapshot.setSourcePath(view.getSourcePath());
        snapshot.setFinishTime(view.getFinishTime());
        return snapshot;
    }

    public GeaflowSnapshot convertView(GraphSnapshotView view) {
        return super.viewToModel(view);
    }
}
