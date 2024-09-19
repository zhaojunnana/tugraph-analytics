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

package com.antgroup.geaflow.console.core.service.file;

import com.aliyun.oss.OSSException;
import com.antgroup.geaflow.console.common.util.NetworkUtil;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.core.model.plugin.GeaflowPlugin;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.model.plugin.config.S3ossPluginConfigClass;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class OssS3FileClient implements RemoteFileClient {

    private S3ossPluginConfigClass ossS3Config;

    private S3Client s3Client;

    @Override
    public void init(GeaflowPlugin plugin, GeaflowPluginConfig config) {
        this.ossS3Config = config.getConfig().parse(S3ossPluginConfigClass.class);
        AwsBasicCredentials credentials = AwsBasicCredentials.create(ossS3Config.getAccessId(),
                ossS3Config.getSecretKey());
        this.s3Client = S3Client.builder().region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .endpointOverride(URI.create(ossS3Config.getEndpoint())).forcePathStyle(true).build();
    }

    @Override
    public void upload(String path, InputStream inputStream) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            BufferedInputStream bis = new BufferedInputStream(inputStream);
            BufferedOutputStream bos = new BufferedOutputStream(byteArrayOutputStream);
            byte[] buffer = new byte[1024];
            int length;
            while ((length = bis.read(buffer)) != -1) {
                bos.write(buffer, 0, length);
            }
            bos.flush();
            bos.close();
            bis.close();
            byte[] copiedData = byteArrayOutputStream.toByteArray();
            s3Client.putObject(PutObjectRequest.builder()
                    .bucket(this.ossS3Config.getBucket())
                    .key(getFullPath(path))
                    .build(), RequestBody.fromBytes(copiedData));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public InputStream download(String path) throws OSSException {
        return s3Client.getObject(GetObjectRequest.builder()
                .bucket(this.ossS3Config.getBucket())
                .key(getFullPath(path))
                .build());
    }

    @Override
    public void delete(String path) {
        s3Client.deleteObject(DeleteObjectRequest.builder()
                .bucket(this.ossS3Config.getBucket())
                .key(getFullPath(path))
                .build());
    }

    @Override
    public String getUrl(String path) {
        return String.format("http://%s.%s/%s", ossS3Config.getBucket(),
                NetworkUtil.getHost(ossS3Config.getEndpoint()),
                getFullPath(path));
    }

    public String getFullPath(String path) {
        String root = ossS3Config.getRoot();
        if (!StringUtils.startsWith(root, "/")) {
            throw new GeaflowException("Invalid root config, should start with /");
        }

        root = StringUtils.removeStart(root, "/");
        root = StringUtils.removeEnd(root, "/");

        return root.isEmpty() ? path : String.format("%s/%s", root, path);
    }

}
