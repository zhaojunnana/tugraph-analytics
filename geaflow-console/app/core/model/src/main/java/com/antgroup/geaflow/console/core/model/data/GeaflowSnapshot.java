package com.antgroup.geaflow.console.core.model.data;

import com.antgroup.geaflow.console.core.model.GeaflowName;

import java.util.Date;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GeaflowSnapshot  extends GeaflowName {

    Long graphId;
    Long instanceId;
    String sourcePath;
    String snapshotPath;
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
