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

package com.antgroup.geaflow.console.core.service.converter;

import com.antgroup.geaflow.console.common.dal.entity.GeaflowSnapshotEntity;
import com.antgroup.geaflow.console.core.model.data.GeaflowSnapshot;
import org.springframework.stereotype.Component;

@Component
public class SnapshotConverter extends DataConverter<GeaflowSnapshot, GeaflowSnapshotEntity> {

    @Override
    public GeaflowSnapshotEntity modelToEntity(GeaflowSnapshot model) {
        GeaflowSnapshotEntity snapshotEntity = super.modelToEntity(model);
        if (model.getTenantId() != null) {
            snapshotEntity.setTenantId(model.getTenantId());
        }
        snapshotEntity.setSnapshotTimeLong(model.getSnapshotTimeLong());
        snapshotEntity.setCheckpoint(model.getCheckpoint());
        snapshotEntity.setSnapshotPath(model.getSnapshotPath());
        snapshotEntity.setSnapshotTime(model.getSnapshotTime());
        snapshotEntity.setInstanceId(model.getInstanceId());
        snapshotEntity.setStatus(model.getStatus().name());
        snapshotEntity.setGraphId(model.getGraphId());
        snapshotEntity.setSourcePath(model.getSourcePath());
        snapshotEntity.setFinishTime(model.getFinishTime());
        return snapshotEntity;
    }

    @Override
    public GeaflowSnapshot entityToModel(GeaflowSnapshotEntity entity) {
        GeaflowSnapshot snapshot = super.entityToModel(entity);
        snapshot.setSnapshotTimeLong(entity.getSnapshotTimeLong());
        snapshot.setCheckpoint(entity.getCheckpoint());
        snapshot.setSnapshotPath(entity.getSnapshotPath());
        snapshot.setSnapshotTime(entity.getSnapshotTime());
        snapshot.setInstanceId(entity.getInstanceId());
        snapshot.setStatus(GeaflowSnapshot.SnapshotStatus.valueOf(entity.getStatus()));
        snapshot.setGraphId(entity.getGraphId());
        snapshot.setSourcePath(entity.getSourcePath());
        snapshot.setFinishTime(entity.getFinishTime());
        return snapshot;
    }

    public GeaflowSnapshot convertEntity(GeaflowSnapshotEntity entity) {
        return entityToModel(entity);
    }
}
