package com.antgroup.geaflow.file.oss;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.GsonUtil;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.file.FileInfo;
import com.antgroup.geaflow.file.PersistentType;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

public class OssS3IO extends OssIO {

    private S3Client s3Client;

    @Override
    public void init(Configuration userConfig) {
        String jsonConfig = Preconditions.checkNotNull(userConfig.getString(FileConfigKeys.JSON_CONFIG));
        Map<String, String> persistConfig = GsonUtil.parse(jsonConfig);

        this.bucketName = Configuration.getString(FileConfigKeys.OSS_BUCKET_NAME, persistConfig);
        String endpoint = Configuration.getString(FileConfigKeys.OSS_ENDPOINT, persistConfig);
        String accessKeyId = Configuration.getString(FileConfigKeys.OSS_ACCESS_ID, persistConfig);
        String accessKeySecret = Configuration.getString(FileConfigKeys.OSS_SECRET_KEY, persistConfig);
        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKeyId, accessKeySecret);
        this.s3Client = S3Client.builder().region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .endpointOverride(URI.create(endpoint)).forcePathStyle(true).build();
    }

    @Override
    public boolean exists(Path path) throws IOException {
        ListObjectsV2Response listObjectsV2Response = s3Client.listObjectsV2(
                ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .prefix(pathToKey(path))
                        .build()
        );
        return !listObjectsV2Response.contents().isEmpty() || !listObjectsV2Response.commonPrefixes().isEmpty();
    }

    @Override
    public void delete(Path path, boolean recursive) throws IOException {
        String key = pathToKey(path);
        if (recursive) {
            String nextMarker = null;
            ListObjectsV2Response objectListing;
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(keyToPrefix(key))
                    .build();
            do {
                request = request.toBuilder().continuationToken(nextMarker).build();
                Preconditions.checkArgument(request.prefix() != null && request.prefix().length() > 0);
                objectListing = s3Client.listObjectsV2(request);
                List<S3Object> contents = objectListing.contents();
                List<String> files = new ArrayList<>();
                for (S3Object s : contents) {
                    files.add(s.key());
                }
                nextMarker = objectListing.nextContinuationToken();
                if (!files.isEmpty()) {
                    files.forEach(f -> s3Client.deleteObject(b -> b.bucket(bucketName).key(f)));
                }
            } while (objectListing.isTruncated());
        } else {
            s3Client.deleteObject(b -> b.bucket(bucketName).key(key));
        }
    }

    @Override
    public boolean rename(Path from, Path to) throws IOException {
        String fromKey = pathToKey(from);
        String toKey = pathToKey(to);
        String nextMarker = null;
        ListObjectsV2Response objectListing;
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(keyToPrefix(fromKey))
                .build();
        do {
            request = request.toBuilder().continuationToken(nextMarker).build();
            Preconditions.checkArgument(request.prefix() != null && request.prefix().length() > 0);
            objectListing = s3Client.listObjectsV2(request);
            List<S3Object> contents = objectListing.contents();
            for (S3Object s : contents) {
                String key = s.key();
                String newKey = key.replace(fromKey, toKey);
                s3Client.copyObject(CopyObjectRequest.builder()
                        .sourceBucket(bucketName)
                        .sourceKey(key)
                        .destinationBucket(bucketName)
                        .destinationKey(newKey)
                        .build());
                s3Client.deleteObject(DeleteObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build());
            }
            nextMarker = objectListing.nextContinuationToken();
        } while (objectListing.isTruncated());

        fromKey = pathToKey(from);
        toKey = pathToKey(to);
        if (!from.toString().endsWith("/") && !to.toString().endsWith("/")
                && exists(new Path(fromKey))) {
            s3Client.copyObject(CopyObjectRequest.builder()
                    .sourceBucket(bucketName)
                    .sourceKey(fromKey)
                    .destinationBucket(bucketName)
                    .destinationKey(toKey)
                    .build());
            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fromKey)
                    .build());
        }
        return true;
    }

    @Override
    public boolean createNewFile(Path path) throws IOException {
        if (exists(path)) {
            return false;
        }
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(pathToKey(path))
                        .build(),
                RequestBody.empty());
        return true;
    }

    @Override
    public void copyFromLocalFile(Path local, Path remote) throws IOException {
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(pathToKey(remote))
                        .build(),
                RequestBody.fromFile(new File(local.toString())));
    }

    @Override
    public void copyToLocalFile(Path remote, Path local) throws IOException {
        FileUtils.copyInputStreamToFile(open(remote), new File(local.toString()));
    }

    @Override
    public void copyRemoteToRemoteFile(Path srcPath, Path dstPath) throws IOException {
        s3Client.copyObject(CopyObjectRequest.builder()
                .sourceBucket(bucketName)
                .sourceKey(pathToKey(srcPath))
                .destinationBucket(bucketName)
                .destinationKey(pathToKey(dstPath))
                .build());
    }

    @Override
    public long getRemoteFileSize(Path path) throws IOException {
        ResponseInputStream<GetObjectResponse> s3ClientObject = s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(pathToKey(path))
                .build());
        return s3ClientObject.response().contentLength();
    }

    @Override
    public long getFileCount(Path path) throws IOException {
        long count = 0;
        String nextMarker = null;
        ListObjectsV2Response objectListing;
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(keyToPrefix(pathToKey(path)))
                .build();
        do {
            request = request.toBuilder().continuationToken(nextMarker).build();
            objectListing = s3Client.listObjectsV2(request);
            count += objectListing.contents().size();
            nextMarker = objectListing.nextContinuationToken();
        } while (objectListing.isTruncated());

        return count;
    }

    @Override
    public FileInfo getFileInfo(Path path) throws IOException {
        ResponseInputStream<GetObjectResponse> s3ClientObject = s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(pathToKey(path))
                .build());
        GetObjectResponse response = s3ClientObject.response();
        return FileInfo.of()
                .withPath(path)
                .withLength(response.contentLength())
                .withModifiedTime(response.lastModified().toEpochMilli());
    }

    @Override
    public FileInfo[] listStatus(Path path) throws IOException {
        Set<FileInfo> res = new HashSet<>();
        String nextMarker = null;
        ListObjectsV2Response objectListing;

        ListObjectsV2Request request = ListObjectsV2Request
                .builder()
                .bucket(bucketName)
                .prefix(keyToPrefix(pathToKey(path)))
                .build();
        int prefixLen = request.prefix().length();
        do {
            request = request.toBuilder().continuationToken(nextMarker).build();
            objectListing = s3Client.listObjectsV2(request);
            List<S3Object> contents = objectListing.contents();
            for (S3Object s : contents) {
                String str = s.key().substring(prefixLen);
                int nextPos = str.indexOf('/');
                Path filePath;
                long modifiedTime;
                if (nextPos == -1) {
                    filePath = new Path(keyToPath(s.key()));
                    modifiedTime = s.lastModified().toEpochMilli();
                } else {
                    filePath = new Path(keyToPath(request.prefix() + str.substring(0, nextPos)));
                    modifiedTime = 0;
                }
                FileInfo fileInfo = FileInfo.of()
                        .withPath(filePath)
                        .withLength(s.size())
                        .withModifiedTime(modifiedTime);
                res.add(fileInfo);
            }
            nextMarker = objectListing.nextContinuationToken();
        } while (objectListing.isTruncated());
        return res.toArray(new FileInfo[0]);
    }

    @Override
    public InputStream open(Path path) throws IOException {
        return s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(pathToKey(path))
                .build());
    }

    @Override
    public void close() throws IOException {
        this.s3Client.close();
    }

    @Override
    public PersistentType getPersistentType() {
        return PersistentType.S3OSS;
    }
}
