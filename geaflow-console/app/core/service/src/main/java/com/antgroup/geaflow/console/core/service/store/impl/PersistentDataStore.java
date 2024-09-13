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

package com.antgroup.geaflow.console.core.service.store.impl;

import com.antgroup.geaflow.console.common.service.integration.engine.Configuration;
import com.antgroup.geaflow.console.common.service.integration.engine.FsPath;
import com.antgroup.geaflow.console.common.service.integration.engine.IPersistentIO;
import com.antgroup.geaflow.console.common.service.integration.engine.PersistentIOBuilder;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.data.GeaflowInstance;
import com.antgroup.geaflow.console.core.model.data.GeaflowSnapshot;
import com.antgroup.geaflow.console.core.model.job.config.PersistentArgsClass;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import com.antgroup.geaflow.console.core.model.version.GeaflowVersion;
import com.antgroup.geaflow.console.core.service.InstanceService;
import com.antgroup.geaflow.console.core.service.PluginConfigService;
import com.antgroup.geaflow.console.core.service.PluginService;
import com.antgroup.geaflow.console.core.service.SnapshotService;
import com.antgroup.geaflow.console.core.service.VersionService;
import com.antgroup.geaflow.console.core.service.runtime.TaskParams;
import com.antgroup.geaflow.console.core.service.store.GeaflowDataStore;
import com.antgroup.geaflow.console.core.service.version.VersionClassLoader;
import com.antgroup.geaflow.console.core.service.version.VersionFactory;

import com.google.common.base.Splitter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PersistentDataStore implements GeaflowDataStore {

    private final ExecutorService shardService = new ThreadPoolExecutor(
            1,
            Runtime.getRuntime().availableProcessors(),
            30,
            TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(Integer.MAX_VALUE),
            new ThreadFactoryBuilder()
                    .setNameFormat("shard-%d")
                    .setDaemon(true)
                    .build()
    );

    @Autowired
    private VersionFactory versionFactory;

    @Autowired
    private PluginConfigService pluginConfigService;

    @Autowired
    private PluginService pluginService;

    @Autowired
    private VersionService versionService;

    @Autowired
    private InstanceService instanceService;

    @Autowired
    private SnapshotService snapshotService;

    @Override
    public Long queryStorageUsage(GeaflowTask task) {
        return null;
    }

    @Override
    public Long queryFileCount(GeaflowTask task) {
        IPersistentIO persistentIO = buildPersistentIO(task.getDataPluginConfig(), task.getRelease().getVersion());
        FsPath path = getTaskPath(task);
        return persistentIO.getFileCount(path);
    }

    @Override
    public Date queryModifyTime(GeaflowTask task) {
        return null;
    }

    @Override
    public void cleanTaskData(GeaflowTask task) {
        IPersistentIO persistentIO = buildPersistentIO(task.getDataPluginConfig(), task.getRelease().getVersion());
        FsPath path = getTaskPath(task);
        persistentIO.delete(path, true);
    }

    @Override
    public void cleanGraphData(GeaflowGraph graph) {
        // use default config
        GeaflowPluginCategory category = GeaflowPluginCategory.DATA;
        String dataType = pluginService.getDefaultPlugin(category).getType();
        GeaflowPluginConfig dataConfig = pluginConfigService.getDefaultPluginConfig(category, dataType);

        GeaflowVersion version = versionService.getDefaultVersion();
        IPersistentIO persistentIO = buildPersistentIO(dataConfig, version);

        PersistentArgsClass persistentArgs = new PersistentArgsClass(dataConfig);
        String root = persistentArgs.getRoot();
        VersionClassLoader classLoader = versionFactory.getClassLoader(version);

        GeaflowInstance instance = instanceService.get(graph.getInstanceId());
        String pathSuffix = instance.getName() + "_" + graph.getName();

        FsPath path = classLoader.newInstance(FsPath.class, root, pathSuffix);
        persistentIO.delete(path, true);
        log.info("clean graph data {},{}", root, pathSuffix);
    }

    @Override
    public void snapshotGraphDate(GeaflowGraph graph, GeaflowSnapshot geaflowSnapshot) {
        // use default config
        GeaflowPluginCategory category = GeaflowPluginCategory.DATA;
        String dataType = pluginService.getDefaultPlugin(category).getType();
        GeaflowPluginConfig dataConfig = pluginConfigService.getDefaultPluginConfig(category, dataType);

        GeaflowVersion version = versionService.getDefaultVersion();
        IPersistentIO persistentIO = buildPersistentIO(dataConfig, version);

        PersistentArgsClass persistentArgs = new PersistentArgsClass(dataConfig);
        if (geaflowSnapshot.getSourcePath() == null || geaflowSnapshot.getSourcePath().isEmpty()) {
            geaflowSnapshot.setSourcePath(persistentArgs.getRoot());
        }
        if (geaflowSnapshot.getSnapshotPath() == null || geaflowSnapshot.getSnapshotPath().isEmpty()) {
            geaflowSnapshot.setSnapshotPath(persistentArgs.getSnapshot());
        }
        VersionClassLoader classLoader = versionFactory.getClassLoader(version);

        GeaflowInstance instance = instanceService.get(graph.getInstanceId());
        String pathSuffix = instance.getName() + "_" + graph.getName();

        FsPath srcPath = classLoader.newInstance(FsPath.class, geaflowSnapshot.getSourcePath(), pathSuffix);
        long snapshotTime = Instant.now().getEpochSecond();
        geaflowSnapshot.setSnapshotTime(new Date(snapshotTime));
        FsPath dstPath = classLoader.newInstance(FsPath.class,
                geaflowSnapshot.getSnapshotPath(), pathSuffix + "/" + snapshotTime);
        shardService.execute(new Thread(() -> {
            copyLastVersionGraphData(persistentIO, classLoader, geaflowSnapshot, srcPath, dstPath);
        }));
        geaflowSnapshot.setStatus(GeaflowSnapshot.SnapshotStatus.RUNNING);
        log.info("snapshot graph data:{},sourcePath:{},snapshotPath:{}", pathSuffix,
                geaflowSnapshot.getSourcePath(), pathSuffix);
        snapshotService.create(geaflowSnapshot);
    }

    @SneakyThrows
    private void copyLastVersionGraphData(IPersistentIO persistentIO, VersionClassLoader classLoader,
                                          GeaflowSnapshot geaflowSnapshot, FsPath srcPath, FsPath dstPath) {
        List<String> srcFileList = persistentIO.listFile(srcPath);
        if (srcFileList == null || srcFileList.isEmpty()) {
            return;
        }
        List<String> srcDataPath = srcFileList.stream()
                .filter(s -> s.matches("\\d+"))
                .collect(Collectors.toList());
        long lastVersion = getLastVersion(persistentIO, classLoader, srcPath, srcDataPath);
        if (lastVersion <= 0) {
            return;
        }
        List<FsPath> shardPathList =  srcDataPath.stream()
                .map(p -> classLoader.newInstance(FsPath.class, srcPath.toString(), p))
                .collect(Collectors.toList());
        List<Future<Boolean>> callList = new ArrayList<>();
        shardPathList.forEach(shardPath -> {
            callList.add(shardService.submit(new ShardSnapshotTask(persistentIO,
                    classLoader, shardPath, dstPath, lastVersion)));
        });

        boolean success = true;
        for (Future<Boolean> future : callList) {
            if (!future.get()) {
                success = false;
                break;
            }
        }
        geaflowSnapshot.setFinishTime(new Date());
        if (success) {
            geaflowSnapshot.setStatus(GeaflowSnapshot.SnapshotStatus.SUCCESS);
        } else {
            geaflowSnapshot.setStatus(GeaflowSnapshot.SnapshotStatus.FAILED);
            persistentIO.delete(dstPath, true);
        }
        snapshotService.update(geaflowSnapshot);
    }

    private long getLastVersion(IPersistentIO persistentIO, VersionClassLoader classLoader,
                                FsPath srcPath, List<String> srcDataPath) {
        if (srcDataPath == null || srcDataPath.isEmpty()) {
            return -1;
        }
        long lastVersion = Long.MAX_VALUE;
        for (String path : srcDataPath) {
            long maxShardVersion = -1;
            FsPath fsPath = classLoader.newInstance(FsPath.class, srcPath.toString(), path);
            List<String> metaPathList = persistentIO.listFile(fsPath);
            if (metaPathList == null || metaPathList.isEmpty()) {
                return -1;
            }
            List<String> metaFiles = metaPathList
                    .stream()
                    .filter(s -> s.startsWith("meta."))
                    .sorted(Comparator.comparingLong(s -> -Long.parseLong(s.split("\\.")[1])))
                    .collect(Collectors.toList());
            if (metaFiles.isEmpty()) {
                return -1;
            }
            for (String metaFile : metaFiles) {
                FsPath metaPath = classLoader.newInstance(
                        FsPath.class,
                        fsPath.toString(),
                        metaFile + "/_commit");
                if (persistentIO.exists(metaPath)) {
                    maxShardVersion = Math.max(maxShardVersion,
                            Long.parseLong(metaFile.split("\\.")[1]));
                    break;
                }
            }
            lastVersion = Math.min(lastVersion, maxShardVersion);
        }
        if (lastVersion == Long.MAX_VALUE) {
            return -1;
        }
        return lastVersion;
    }

    protected IPersistentIO buildPersistentIO(GeaflowPluginConfig pluginConfig, GeaflowVersion version) {
        PersistentArgsClass persistentArgs = new PersistentArgsClass(pluginConfig);
        Map<String, String> config = persistentArgs.build().toStringMap();

        VersionClassLoader classLoader = versionFactory.getClassLoader(version);
        Configuration configuration = classLoader.newInstance(Configuration.class, config);
        PersistentIOBuilder builder = classLoader.newInstance(PersistentIOBuilder.class);
        return builder.build(configuration);
    }

    protected FsPath getTaskPath(GeaflowTask task) {
        PersistentArgsClass persistentArgs = new PersistentArgsClass(task.getDataPluginConfig());
        String root = persistentArgs.getRoot();
        String pathSuffix = TaskParams.getRuntimeTaskName(task.getId());
        VersionClassLoader classLoader = versionFactory.getClassLoader(task.getRelease().getVersion());
        return classLoader.newInstance(FsPath.class, root, pathSuffix);
    }

    static class ShardSnapshotTask implements Callable<Boolean> {
        private final ExecutorService copyFileService;
        private final IPersistentIO persistentIO;
        private final VersionClassLoader classLoader;
        private final FsPath shardPath;
        private final long shard;
        private final FsPath dstPath;
        private final long version;

        public ShardSnapshotTask(IPersistentIO persistentIO, VersionClassLoader classLoader,
                                 FsPath shardPath, FsPath dstPath, long version) {
            this.persistentIO = persistentIO;
            this.classLoader = classLoader;
            this.shardPath = shardPath;
            this.dstPath = dstPath;
            this.version = version;
            String[] shardArray = shardPath.toString().split("/");
            this.shard = Long.parseLong(shardArray[shardArray.length - 1]);
            String threadName = "snapshot-" + this.shard + "-%d";
            copyFileService = new ThreadPoolExecutor(
                    1,
                    Runtime.getRuntime().availableProcessors(),
                    30,
                    TimeUnit.MINUTES,
                    new LinkedBlockingQueue<>(Integer.MAX_VALUE),
                    new ThreadFactoryBuilder()
                            .setNameFormat(threadName)
                            .setDaemon(true)
                            .build()
            );
        }

        @Override
        public Boolean call() throws Exception {
            FsPath metaPath = classLoader.newInstance(FsPath.class,
                    shardPath.toString(), "meta." + version);
            FsPath metaFilesPath = classLoader.newInstance(FsPath.class,
                    metaPath.toString(), "FILES");

            InputStream in = persistentIO.open(metaFilesPath);
            String sstString = IOUtils.toString(in, Charset.defaultCharset());
            List<String> sstList = Splitter.on(",").omitEmptyStrings().splitToList(sstString);
            List<FsPath> copySstList = sstList.stream()
                    .map(sst -> classLoader.newInstance(FsPath.class,
                            shardPath.toString(), "datas/" + sst))
                    .collect(Collectors.toList());
            List<FsPath> dstDataPathList = sstList.stream()
                    .map(sst -> classLoader.newInstance(FsPath.class,
                            dstPath.toString(), shard + "/datas/" + sst))
                    .collect(Collectors.toList());
            List<Future<Boolean>> copyCallList = new ArrayList<>();
            for (int i = 0; i < copySstList.size(); i++) {
                copyCallList.add(copyFileService.submit(snapshotCopyFile(copySstList.get(i),
                        dstDataPathList.get(i), true)));
            }
            FsPath dstMetaPath = classLoader.newInstance(FsPath.class,
                    dstPath.toString(), shard + "/meta." + version);
            copyCallList.add(copyFileService.submit(snapshotCopyFile(metaPath, dstMetaPath, false)));
            for (Future<Boolean> callFuture : copyCallList) {
                if (!callFuture.get()) {
                    return false;
                }
            }
            return true;
        }

        private Callable<Boolean> snapshotCopyFile(final FsPath from, final FsPath to, boolean checkSize) {
            return () -> {
                int count = 0;
                int maxTries = 3;
                boolean checkRes = true;
                while (true) {
                    try {
                        persistentIO.copyRemoteToRemoteFile(from, to);
                        if (checkSize) {
                            checkRes = checkSizeSame(from, to);
                        }
                        if (!checkRes) {
                            if (++count == maxTries) {
                                String msg = "snapshot file size not same: " + from;
                                throw new GeaflowException(msg);
                            }
                        } else {
                            break;
                        }
                    } catch (IOException ex) {
                        if (++count == maxTries) {
                            throw ex;
                        }
                    }
                }
                return checkRes;
            };
        }

        private Boolean checkSizeSame(final FsPath from, final FsPath to) throws IOException {
            return persistentIO.getRemoteFileSize(from) == persistentIO.getRemoteFileSize(to);
        }

    }

}
