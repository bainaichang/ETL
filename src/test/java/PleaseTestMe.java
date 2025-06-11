import cn.hutool.core.io.FileUtil;
import cn.hutool.json.JSONUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import core.Scheduler;
import org.gugu.etl.StartUp;
import org.junit.jupiter.api.Test;
import runtask.Step;
import runtask.StepList;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

public class PleaseTestMe {
    @Test
    public void test_csv_etl_flow() {
        Step input = new Step();
        input.withStepId(1)
             .withDes("读取csv文件")
             .withDomain("input")
             .withSubType("csv")
             .withConfig("filePath", "src/test/java/gugugu.csv")
             .withConfig("delimiter", ",")
             .withConfig("quoteChar", "\"")
             .withConfig("hasHeader", true);
        
        Step output = new Step();
        output.withStepId(2)
              .withDes("输出到控制台")
              .withDomain("output")
              .withSubType("console")
              .withParentStepId(Collections.singletonList("1"));
        
        StepList stepList = new StepList(Arrays.asList(input, output));
        
        new Scheduler(stepList).execute();
    }
    
    @Test
    public void testStepList() {
        Step input = new Step();
        input.withStepId(1)
             .withDes("读取csv文件")
             .withDomain("input")
             .withSubType("csv")
             .withConfig("filePath", "src/test/java/gugugu.csv")
             .withConfig("delimiter", ",")
             .withConfig("quoteChar", "\"")
             .withConfig("hasHeader", true);
        
        Step output = new Step();
        output.withStepId(2)
              .withDes("输出到控制台")
              .withDomain("output")
              .withSubType("console")
              .withParentStepId(Collections.singletonList("1"));
        
        Step output_1 = new Step();
        output_1.withStepId(2)
                .withDes("yYy")
                .withDomain("output")
                .withSubType("xxx")
                .withParentStepId(Collections.singletonList("1"));
        StepList sl = new StepList();
        sl.addStep(input);
        sl.addStep(output);
        sl.updateStep(output_1);
        sl.rmStep(input.getStepId()
                       .toString());
        System.out.println(sl);
    }
    
    @Test
    public void test_JSONToBean() throws JsonProcessingException {
        File data = new File("C:\\Users\\白乃常\\.etl\\process_files\\a001.json");
        StringBuffer sb = new StringBuffer();
        FileUtil.readLines(data, StandardCharsets.UTF_8)
                .forEach(sb::append);
        String json = sb.toString();
        ObjectMapper mapper = new ObjectMapper();
        StepList response = mapper.readValue(json, StepList.class);
        System.out.println(response);
    }
}
