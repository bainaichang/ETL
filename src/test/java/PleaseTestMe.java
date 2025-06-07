import core.Scheduler;
import org.junit.jupiter.api.Test;
import runtask.Step;
import runtask.StepList;

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
        sl.rmStep(input.getStepId().toString());
        System.out.println(sl);
    }
}
