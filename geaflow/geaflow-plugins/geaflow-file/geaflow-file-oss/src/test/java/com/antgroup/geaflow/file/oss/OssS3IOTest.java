package com.antgroup.geaflow.file.oss;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.GsonUtil;
import com.antgroup.geaflow.file.FileConfigKeys;
import com.antgroup.geaflow.file.FileInfo;
import com.antgroup.geaflow.file.IPersistentIO;
import com.antgroup.geaflow.file.PersistentIOBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OssS3IOTest {

    @Test(enabled = false)
    public void test() throws Exception {
        FileUtils.touch(new File("/tmp/README"));

        IPersistentIO persistentIO = PersistentIOBuilder.build(getOSSConfig());
        String myName = "myName" + System.currentTimeMillis();
        persistentIO.delete(new Path("/geaflow/chk/" + myName + "2"), true);

        for (int i = 0; i < 101; i++) {
            persistentIO.copyFromLocalFile(new Path("/tmp/README"),
                    new Path("/geaflow/chk/" + myName + "/datas/README" + i));
        }
        persistentIO.copyFromLocalFile(new Path("/tmp/README"), new Path(
                "/geaflow/chk/" + myName + "/0/README"));
        persistentIO.copyFromLocalFile(new Path("/tmp/README"), new Path(
                "/geaflow/chk/" + myName + "/1/README"));

        persistentIO.rename(new Path("/geaflow/chk/" + myName + "/"), new Path(
                "/geaflow/chk/" + myName + "2"));
        List<String> list = persistentIO.listFile(new Path("/geaflow/chk/" + myName + "2"));
        Assert.assertEquals(list.size(), 3);

        FileInfo[] res = persistentIO.listStatus(new Path("/geaflow/chk/" + myName + "2/datas"));
        Assert.assertEquals(res.length, 101);

        persistentIO.rename(new Path("/geaflow/chk/" + myName + "2/datas/README46"),
                new Path("/geaflow/chk/" + myName + "2/datas/MYREADME46"));
        Assert.assertTrue(persistentIO.exists(new Path(
                "/geaflow/chk/" + myName + "2/datas/MYREADME46")));
        Assert.assertFalse(persistentIO.exists(new Path(
                "/geaflow/chk/" + myName + "2/datas/README46")));

        persistentIO.delete(new Path("/geaflow/chk/" + myName + "2"), true);

    }

    private static Configuration getOSSConfig() throws Exception {
        Map<String, String> config = new HashMap<>();
        // set private account info.
        config.put(FileConfigKeys.OSS_BUCKET_NAME.getKey(), "hparg-geaflow");
        config.put(FileConfigKeys.OSS_ENDPOINT.getKey(), "http://s3-tcloud-ht.kanzhun.tech");

        config.put(FileConfigKeys.OSS_ACCESS_ID.getKey(), "GMXLYVDZWKC05J8R1ITO");
        config.put(FileConfigKeys.OSS_SECRET_KEY.getKey(), "mKOqjb1wTXBtV7iMvDQdSP3ye0x9czCgIEWf6Gro");
        Configuration configuration = new Configuration();
        configuration.put(FileConfigKeys.PERSISTENT_TYPE, "S3OSS");
        configuration.put(FileConfigKeys.JSON_CONFIG, GsonUtil.toJson(config));
        return configuration;
    }

}
