package org.gugu.etl.controller;

import runtask.Step;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class StepController {
    @PostMapping("/add_step")
    public String addStep(@RequestBody Step step, @RequestParam String filename){
//        File data = new File()
        return null;
    }
}
