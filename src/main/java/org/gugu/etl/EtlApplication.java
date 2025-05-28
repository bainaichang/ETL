package org.gugu.etl;

import core.Checker;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class EtlApplication {
    static {
        Checker.run("plugin", "anno");
    }
    public static void main(String[] args) {
        SpringApplication.run(EtlApplication.class, args);
    }

}
