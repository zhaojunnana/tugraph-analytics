package com.antgroup.geaflow.console.web.controller.api;

import com.antgroup.geaflow.console.biz.shared.GraphSnapShotManager;
import com.antgroup.geaflow.console.biz.shared.view.GraphSnapshotView;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.common.dal.model.SnapshotSearch;
import com.antgroup.geaflow.console.core.model.data.GeaflowSnapshot;
import com.antgroup.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class GraphSnapshotController {

    @Autowired
    private GraphSnapShotManager graphSnapshotManager;

    @GetMapping("/snapshots")
    public GeaflowApiResponse<PageList<GraphSnapshotView>> search(SnapshotSearch snapshotSearch) {
        return GeaflowApiResponse.success(graphSnapshotManager.search(snapshotSearch));
    }

    @DeleteMapping("/instances/{instanceName}/graph/{graphName}/snapshot/{snapshotName}")
    public GeaflowApiResponse<Boolean> delete(@PathVariable("instanceName") String instanceName,
                                              @PathVariable("graphName") String graphName,
                                              @PathVariable("snapshotName") String snapshotName) {
        return GeaflowApiResponse.success(graphSnapshotManager.delete(instanceName, graphName, snapshotName));
    }

    @PostMapping("/instances/{instanceName}/graph/{graphName}/snapshot")
    public GeaflowApiResponse<Boolean> snapshot(@PathVariable("instanceName") String instanceName,
                                                @PathVariable("graphName") String graphName,
                                                @RequestBody GeaflowSnapshot geaflowSnapshot) {
        return GeaflowApiResponse.success(graphSnapshotManager.snapshot(instanceName, graphName, geaflowSnapshot));
    }
}
