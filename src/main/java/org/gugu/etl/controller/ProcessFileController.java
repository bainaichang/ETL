package org.gugu.etl.controller;

import cn.hutool.core.io.FileUtil;
import cn.hutool.json.JSONUtil;
import org.gugu.etl.StartUp;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Controller
public class ProcessFileController {
    @GetMapping("/getAll")
    @ResponseBody
    public String getAll() {
        HashMap<Object, Object> result = new HashMap<>();
        File[] files = FileUtil.ls(StartUp.dataDir);
        List<String> filenames = new ArrayList<>();
        for (File file : files) {
            filenames.add(file.getName());
        }
        result.put("status", 200);
        result.put("message", "ok");
        result.put("files", JSONUtil.toJsonStr(filenames));
        result.put("filepath",StartUp.dataDir);
        return JSONUtil.toJsonStr(result);
    }
    
    @GetMapping("/cat_file")
    @ResponseBody
    public String catFile(@RequestParam(required = true) String filename) {
        HashMap<Object, Object> result = new HashMap<>();
        File data = new File(StartUp.dataDir + StartUp.fileSeparator + filename);
        if (! FileUtil.exist(data)) {
            result.put("status", 400);
            result.put("message", "file is not exist");
            return JSONUtil.toJsonStr(result);
        }
        StringBuffer sb = new StringBuffer();
        FileUtil.readLines(data, StandardCharsets.UTF_8)
                .forEach(sb::append);
        return sb.toString();
    }
    @PostMapping("/create")
    @ResponseBody
    public String create(@RequestParam String filename){
        HashMap<Object, Object> result = new HashMap<>();
        File data = new File(StartUp.dataDir + StartUp.fileSeparator + filename);
        if (FileUtil.exist(data)) {
            result.put("status", 403);
            result.put("message", "file is exist");
            return JSONUtil.toJsonStr(result);
        }
        FileUtil.touch(filename);
        result.put("status",200);
        result.put("message","ok");
        result.put("filepath",data.getAbsoluteFile());
        return JSONUtil.toJsonStr(result);
    }
}
