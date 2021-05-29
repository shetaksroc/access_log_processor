package com.example.logprocessor;

import com.example.logprocessor.service.AccessLogProcessorService;
import com.example.logprocessor.service.impl.AccessLogProcessorServiceImpl;

import java.io.IOException;

/**
 * @author akshay on 29/05/21
 */
public class MainApp {
    AccessLogProcessorService accessLogProcessorService = new AccessLogProcessorServiceImpl();

    public static void main(String[] args) throws IOException {
        System.out.println("Initializing access log processor");
        MainApp mainApp = new MainApp();
        Long distinctPaths  = Long.parseLong(System.getProperty("distinctPaths", "30"));
        boolean strictSearch  = Boolean.parseBoolean(System.getProperty("strictSearch", "false"));
        System.out.println("Searching for users who has accessed distinct paths="+distinctPaths);
        System.out.println("StrictSearch="+strictSearch);
        mainApp.process(distinctPaths, strictSearch);
    }

    private void process(Long distinctPaths, boolean strictSearch) throws IOException {
        accessLogProcessorService.analyze(distinctPaths, strictSearch);
    }
}
