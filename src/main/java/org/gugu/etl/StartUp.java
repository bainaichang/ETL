package org.gugu.etl;

import cn.hutool.core.io.FileUtil;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.file.FileSystems;

@Component
public class StartUp {
    public static String dataDir = null;
    public static String fileSeparator = FileSystems.getDefault()
                                                    .getSeparator();
    
    @PostConstruct
    void dataDirInit() {
        StringBuilder sb = new StringBuilder(System.getProperty("user.home"));
        sb.append(fileSeparator)
          .append(".etl")
          .append(fileSeparator)
          .append("process_files");
        dataDir = sb.toString();
        if (! FileUtil.exist(dataDir)) {
            FileUtil.touch(dataDir);
            System.out.println("数据目录创建成功！");
        } else {
            System.out.println("数据目录已存在");
        }
    }
}
