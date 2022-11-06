package com.bytedance.primus.io.datasource.file.impl.customized;

import com.bytedance.primus.io.datasource.file.FileDataSource;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;

public class CustomizedFileDataSourceTest {

  @Test
  public void testCreateRecordReader() {
    FileDataSource source = new CustomizedFileDataSource(
        new HashMap<String, String>() {{
          put("key-prefix", "key-prefix");
          put("val-prefix", "val-prefix");
          put("key-suffix", "key-suffix");
          put("val-suffix", "val-suffix");
        }});
    Assert.assertNotNull(source);
  }
}
