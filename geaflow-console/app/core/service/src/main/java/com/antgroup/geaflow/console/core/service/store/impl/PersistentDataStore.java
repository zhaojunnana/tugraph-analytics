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
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.core.model.data.GeaflowGraph;
import com.antgroup.geaflow.console.core.model.data.GeaflowInstance;
import com.antgroup.geaflow.console.core.model.job.config.PersistentArgsClass;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import com.antgroup.geaflow.console.core.model.version.GeaflowVersion;
import com.antgroup.geaflow.console.core.service.InstanceService;
import com.antgroup.geaflow.console.core.service.PluginConfigService;
import com.antgroup.geaflow.console.core.service.PluginService;
import com.antgroup.geaflow.console.core.service.VersionService;
import com.antgroup.geaflow.console.core.service.runtime.TaskParams;
import com.antgroup.geaflow.console.core.service.store.GeaflowDataStore;
import com.antgroup.geaflow.console.core.service.version.VersionClassLoader;
import com.antgroup.geaflow.console.core.service.version.VersionFactory;

import com.google.common.base.Splitter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
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
    public void snapshotGraphDate(GeaflowGraph graph, String sourcePath, String snapshotPath) {
        // use default config
        GeaflowPluginCategory category = GeaflowPluginCategory.DATA;
        String dataType = pluginService.getDefaultPlugin(category).getType();
        GeaflowPluginConfig dataConfig = pluginConfigService.getDefaultPluginConfig(category, dataType);

        GeaflowVersion version = versionService.getDefaultVersion();
        IPersistentIO persistentIO = buildPersistentIO(dataConfig, version);

        PersistentArgsClass persistentArgs = new PersistentArgsClass(dataConfig);
        if (sourcePath == null || sourcePath.isEmpty()) {
            sourcePath = persistentArgs.getRoot();
        }
        if (snapshotPath == null || snapshotPath.isEmpty()) {
            snapshotPath = persistentArgs.getSnapshot();
        }
        VersionClassLoader classLoader = versionFactory.getClassLoader(version);

        GeaflowInstance instance = instanceService.get(graph.getInstanceId());
        String pathSuffix = instance.getName() + "_" + graph.getName();

        FsPath srcPath = classLoader.newInstance(FsPath.class, sourcePath, pathSuffix);
        FsPath dstPath = classLoader.newInstance(FsPath.class, snapshotPath, pathSuffix);
        copyLastVersionGraphData(persistentIO, classLoader, srcPath, dstPath);
        log.info("snapshot graph data:{},sourcePath:{},snapshotPath:{}", pathSuffix, sourcePath, pathSuffix);
    }

    private void copyLastVersionGraphData(IPersistentIO persistentIO, VersionClassLoader classLoader,
                                          FsPath srcPath, FsPath dstPath) {
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
        shardPathList.forEach(shardPath -> {
            shardService.submit(new ShardSnapShotTask(persistentIO, classLoader, shardPath, dstPath, lastVersion));
        });

    }

    private long getLastVersion(IPersistentIO persistentIO, VersionClassLoader classLoader,
                                FsPath srcPath, List<String> srcDataPath) {
        if (srcDataPath == null || srcDataPath.isEmpty()) {
            return -1;
        }
        long lasVersion = Long.MAX_VALUE;
        for (String path : srcDataPath) {
            FsPath fsPath = classLoader.newInstance(FsPath.class, srcPath.toString(), path);
            List<String> metaPathList = persistentIO.listFile(fsPath);
            if (metaPathList == null || metaPathList.isEmpty()) {
                return -1;
            }
            List<String> metaFiles = metaPathList
                    .stream()
                    .filter(s -> s.startsWith("meta."))
                    .sorted(Comparator.comparingLong(s -> Long.parseLong(s.split("\\.")[1])))
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
                    lasVersion = Math.min(lasVersion,
                            Long.parseLong(metaFile.split("\\.")[1]));
                }
            }
        }
        if (lasVersion == Long.MAX_VALUE) {
            return -1;
        }
        return lasVersion;
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

    static class ShardSnapShotTask implements Runnable {
        private final ExecutorService copyFileService;
        private final IPersistentIO persistentIO;
        private final VersionClassLoader classLoader;
        private final FsPath shardPath;
        private final long shard;
        private final FsPath dstPath;
        private final long version;

        public ShardSnapShotTask(IPersistentIO persistentIO, VersionClassLoader classLoader,
                                 FsPath shardPath, FsPath dstPath, long version) {
            this.persistentIO = persistentIO;
            this.classLoader = classLoader;
            this.shardPath = shardPath;
            this.dstPath = dstPath;
            this.version = version;
            String[] shardArray = shardPath.toString().split("/");
            this.shard = Long.parseLong(shardArray[shardArray.length - 1]);
            String threadName = "snapshot" + this.shard + "-%d";
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

        @SneakyThrows
        @Override
        public void run() {
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
            FsPath dstDataPath = classLoader.newInstance(FsPath.class,
                    dstPath.toString(), shard + "/datas");
            for (FsPath copySstPath : copySstList) {
                copyFileService.submit(snapshotCopyFile(copySstPath, dstDataPath));
            }

            FsPath dstMetaPath = classLoader.newInstance(FsPath.class,
                    dstPath.toString(), shard + "/meta." + version);
            copyFileService.submit(snapshotCopyFile(metaPath, dstMetaPath));
        }

        private Callable<Long> snapshotCopyFile(final FsPath from, final FsPath to) {
            return () -> {
                persistentIO.copyRemoteToRemoteFile(from, to);
                return 1L;
            };
        }

    }

}
