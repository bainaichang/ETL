package org.gugu.etl.controller;

import cn.hutool.core.io.FileUtil;
import cn.hutool.json.JSONUtil;
import org.gugu.etl.StartUp;
import org.springframework.web.bind.annotation.ResponseBody;
import runtask.Step;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import runtask.StepList;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

@Controller
public class StepController {
    @PostMapping("/add_step")
    @ResponseBody
    public String addStep(@RequestBody Step step, @RequestParam String filename){
        HashMap<Object, Object> result = new HashMap<>();
        StepList sl = getStepList(filename);
        sl.addStep(step);
        result.put("status","200");
        result.put("message","ok");
        return JSONUtil.toJsonStr(result);
    }
    @PostMapping("/rm_step")
    @ResponseBody
    public String rmStep(@RequestParam String filename,@RequestParam String stepId){
        StepList sl = getStepList(filename);
        boolean b = sl.rmStep(stepId);
        HashMap<Object, Object> result = new HashMap<>();
        if (b) {
            result.put("status","200");
            result.put("message","ok");
        } else {
            result.put("status","400");
            result.put("message","step remove fail");
        }
        return JSONUtil.toJsonStr(result);
    }
    
    @PostMapping("/update_step")
    @ResponseBody
    public String updateStep(@RequestBody Step step, @RequestParam String filename){
        HashMap<Object, Object> result = new HashMap<>();
        StepList sl = getStepList(filename);
        boolean b = sl.updateStep(step);
        if (b) {
            result.put("status","200");
            result.put("message","ok");
        } else {
            result.put("status","400");
            result.put("message","step remove fail");
        }
        return JSONUtil.toJsonStr(result);
    }
    
    private static StepList getStepList(String filename) {
        File data = new File(StartUp.dataDir + StartUp.fileSeparator + filename);
        StringBuffer sb = new StringBuffer();
        FileUtil.readLines(data, StandardCharsets.UTF_8)
                .forEach(sb::append);
        String json = sb.toString();
        return JSONUtil.toBean(json, StepList.class);
    }
}
