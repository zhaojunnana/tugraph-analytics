package com.antgroup.geaflow.console.core.model.data;

import java.util.Date;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GeaflowSnapshot  extends GeaflowData {

    String graphId;
    String sourcePath;
    String snapshotPath;
    Long snapshotTimeLong;
    Long checkpoint;
    Date snapshotTime;
    Date finishTime;
    SnapshotStatus status;

    public enum SnapshotStatus {
        INIT,
        RUNNING,
        FAILED,
        SUCCESS
    }

}
