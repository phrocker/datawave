package datawave.queryrunner;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class QueryPropertiesFileTest {


    @Test
    public void testCreateQueryPropertiesFile() throws IOException {
        QueryPropertiesFile qpf = new QueryPropertiesFile();
        qpf.setProperty("maxIndexBatchSize",26);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        qpf.store(baos,"outputstream");
        System.out.println(new String(baos.toByteArray()));
    }

    @Test
    public void testLoadPropertiesFile() throws IOException {
        QueryPropertiesFile qpf = new QueryPropertiesFile();
        qpf.setProperty("maxIndexBatchSize",26);
        qpf.setProperty("maxIndexScanTimeMillis",27L);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        qpf.store(baos,"outputstream");

        // load the file, now
        qpf = new QueryPropertiesFile();
        qpf.load(new ByteArrayInputStream(baos.toByteArray()));

        Assert.assertEquals(27,qpf.getShardQueryConfiguration().getMaxIndexScanTimeMillis());
        Assert.assertEquals(26,qpf.getShardQueryConfiguration().getMaxIndexBatchSize());
    }
}
