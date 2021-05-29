package com.example.logprocessor.service;

import java.io.IOException;

/**
 * @author akshay on 29/05/21
 */
public interface AccessLogProcessorService {
    void analyze(Long N, boolean strictSearch) throws IOException;
}
