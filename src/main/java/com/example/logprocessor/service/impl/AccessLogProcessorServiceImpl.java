package com.example.logprocessor.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import com.example.logprocessor.service.AccessLogProcessorService;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;

/**
 * @author akshay on 29/05/21
 */
public class AccessLogProcessorServiceImpl implements AccessLogProcessorService {
    public static long MAX_CHUNK_SIZE = 20000L;
    public static final String OUTPUT_DIR="/tmp/processing/";
    public static final String LEVEL_DIR=OUTPUT_DIR+"%s/";
    public static final String FILE_CHUNK_FORMAT=LEVEL_DIR+"chunk_%s";
    public static final String FINAL_OUTPUT_DIR="/tmp/processing/output/";
    public static final String FINAL_OUTPUT_FILE_CHUNK=FINAL_OUTPUT_DIR+"chunk_%s";
    public static final String ACCESS_LOG_DIR="src/main/resources/access_logs/access_large.log";
    public static final String LOG_SEPARATOR=":";

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void analyze(Long distinctPaths, boolean strictSearch) throws IOException {
        int level = 0;
        final Map<String, Set<String>> userLevelAccessLogDetails = new TreeMap<>();
        final int[] chunkIndex={0};

        createDir(OUTPUT_DIR);
        createDir(String.format(LEVEL_DIR, level));

        try(
                InputStream inputStream = new FileInputStream(ACCESS_LOG_DIR);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                Stream<String> linesStream = bufferedReader.lines()) {

            preProcessing(level, userLevelAccessLogDetails, chunkIndex, linesStream);
            System.out.println("Pre processing on the access log files is completed");
            int finalLevel = mergeAccessLog(level);
            processFinalOutput(finalLevel, distinctPaths, strictSearch);
            System.out.println("Log processing is complete");
        }
    }

    private void preProcessing(int level, Map<String, Set<String>> userLevelAccessLogDetails, int[] chunkIndex, Stream<String> linesStream) throws IOException {
        linesStream.forEach(line -> {
            String[] data =  line.split(",");
            userLevelAccessLogDetails.computeIfAbsent(data[1], a -> new HashSet<String>()).add(data[2]);
            if(userLevelAccessLogDetails.size()== MAX_CHUNK_SIZE){
                try {
                    writeData( userLevelAccessLogDetails, level, chunkIndex[0]);
                    userLevelAccessLogDetails.clear();
                    chunkIndex[0]++;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        if(userLevelAccessLogDetails.size()>0 && userLevelAccessLogDetails.size()< MAX_CHUNK_SIZE){
            writeData( userLevelAccessLogDetails, level, chunkIndex[0]);
        }
    }

    private void createDir(String path) throws IOException {
        File pathPointer = new File(path);
        FileUtils.deleteDirectory(pathPointer);
        FileUtils.forceMkdir(pathPointer);
    }

    /**
     * Identifying users who has visited distinct paths equal to the queried number and writing it to a file
     * @param level
     * @param distinctPaths
     * @throws IOException
     */
    private void processFinalOutput(int level, long distinctPaths, boolean strictSearch) throws IOException {
        createDir(FINAL_OUTPUT_DIR);

        File pathAsFile = new File(String.format(LEVEL_DIR, level));
        File[] files = pathAsFile.listFiles();
        int finalOutputChunk=0;

        List<String> output = new ArrayList<>();
        for(File file: files){
            try (
                    InputStream inputStream = new FileInputStream(file);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            ) {
                String line = bufferedReader.readLine();
                while(line!=null) {
                    String[] data = line.split(LOG_SEPARATOR);
                    String userId = data[0];
                    Set<String> paths = objectMapper.readValue(data[1], new TypeReference<Set<String>>() {});
                    /**
                     * Considering users who has visited atleast N distinctPaths
                     * Supporting strictSearch in case we want to get only the users who has visited paths=distinctPaths
                     */
                    if(strictSearch){
                        if(paths.size()==distinctPaths) {
                            output.add(userId);
                        }
                    }else if(paths.size()>=distinctPaths){
                        output.add(userId);
                    }
                    if(output.size()== MAX_CHUNK_SIZE){
                        writeFinalOutput(finalOutputChunk, output);
                    }
                    line = bufferedReader.readLine();
                }
            }
        }
        if(output.size()>0 && output.size()< MAX_CHUNK_SIZE){
            writeFinalOutput(finalOutputChunk, output);
        }

        System.out.println("Output files can be found in the location:"+FINAL_OUTPUT_DIR);
    }

    private void writeFinalOutput(int finalOutputChunk, List<String> output) throws IOException {
        final BufferedWriter[] bf=new BufferedWriter[1];
        bf[0]=null;
        try {
            File fileOp = new File(String.format(FINAL_OUTPUT_FILE_CHUNK, finalOutputChunk));
            fileOp.createNewFile();
            bf[0] = new BufferedWriter(new FileWriter(fileOp));
            output.parallelStream()
                    .forEach(user -> {
                        try {
                            bf[0].write(user+"\n");
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
            output.clear();
            finalOutputChunk++;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }finally {
            if(Objects.nonNull(bf[0])){
                bf[0].close();
            }
        }
    }

    private int mergeAccessLog(int processingLevel) throws IOException {

        File mergeLevelDirPath = new File(String.format(LEVEL_DIR, processingLevel));
        File[] files = mergeLevelDirPath.listFiles();

        while(files.length!=1){
            mergeLevelDirPath = new File(String.format(LEVEL_DIR, processingLevel));
            files = mergeLevelDirPath.listFiles();
            int interval = 1, i, fileChunkIndexAtCurrentLevel=0;
            processingLevel=processingLevel+1;
            /**
             * Creating next level directory for storing output files
             */
            createDir(String.format(LEVEL_DIR, processingLevel));

            /**
             * Merging 2 chunks at a time
             */
            for(i=0; i+interval<files.length; i=i+interval*2){
                fileChunkIndexAtCurrentLevel=mergeTwoFiles(files[i], files[i+interval], processingLevel, fileChunkIndexAtCurrentLevel);
            }
            if(i==files.length-1){
                writeSingleFileInNextLeve(files[files.length-1], processingLevel, fileChunkIndexAtCurrentLevel);
            }

            /**
             * Cleaning up already processed temp directories, Optimizing storage
             */
            FileUtils.deleteDirectory(new File(String.format(LEVEL_DIR, processingLevel-1)));
        }
        return processingLevel;
    }


    private Integer writeSingleFileInNextLeve(File file, int processingLevel, int chunk) throws IOException {
        Map<String, Set<String>> userCount = new TreeMap<>();
        try (
                InputStream inputStream = new FileInputStream(file);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        ) {
            String line1 = bufferedReader.readLine();
            while(line1!=null){
                try {
                    String[] data = line1.split(LOG_SEPARATOR);
                    groupAccessLogAtUserIdLevel(userCount, data);
                    line1 = bufferedReader.readLine();
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }

            if(userCount.size()>0){
                writeData(userCount, processingLevel, chunk);
                chunk++;
            }
        }
        return chunk;
    }


    private Integer mergeTwoFiles(File file1, File file2, int processingLevel, int fileChunkIndexAtCurrentLevel) throws IOException {
        Map<String, Set<String>> userIdLevelAccessLog = new TreeMap<>();
        try (
                BufferedReader file1bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file1)));
                BufferedReader file2BufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file2)));
        ) {
            String lineInFile1 = file1bufferedReader.readLine();
            String lineInFile2 = file2BufferedReader.readLine();

            while(lineInFile1!=null && lineInFile2!=null){
                String[] data = lineInFile1.split(LOG_SEPARATOR);
                groupAccessLogAtUserIdLevel(userIdLevelAccessLog, data);
                data = lineInFile2.split(LOG_SEPARATOR);
                groupAccessLogAtUserIdLevel(userIdLevelAccessLog, data);
                lineInFile1 = file1bufferedReader.readLine();
                lineInFile2 = file2BufferedReader.readLine();
            }

            while(lineInFile1!=null){
                String[] data = lineInFile1.split(LOG_SEPARATOR);
                groupAccessLogAtUserIdLevel(userIdLevelAccessLog, data);
                lineInFile1 = file1bufferedReader.readLine();
            }

            while(lineInFile2!=null){
                String[] data = lineInFile2.split(LOG_SEPARATOR);
                groupAccessLogAtUserIdLevel(userIdLevelAccessLog, data);
                lineInFile2 = file2BufferedReader.readLine();
            }
            writeData(userIdLevelAccessLog, processingLevel, fileChunkIndexAtCurrentLevel++);
        }
        return fileChunkIndexAtCurrentLevel;
    }

    private void groupAccessLogAtUserIdLevel(Map<String, Set<String>> userCount, String[] data) throws JsonProcessingException {
        if (userCount.containsKey(data[0])) {
            userCount.get(data[0]).addAll(objectMapper.readValue(data[1], new TypeReference<Set<String>>() {}));
        } else {
            userCount.put(data[0], objectMapper.readValue(data[1], new TypeReference<Set<String>>() {}));
        }
    }

    private void writeData(Map<String, Set<String>> userIdLevelAccessData, int processingLevel, int fileChunkIndex) throws IOException {
        File file = new File(String.format(FILE_CHUNK_FORMAT,processingLevel, fileChunkIndex));
        BufferedWriter writer = null;
        try {
            file.createNewFile();
            writer = new BufferedWriter(new FileWriter(file));;
            for (Map.Entry<String, Set<String>> entry : userIdLevelAccessData.entrySet()) {
                writer.write(entry.getKey() + LOG_SEPARATOR + objectMapper.writeValueAsString(entry.getValue())+"\n");
            }
        } finally {
            if (Objects.nonNull(writer)){
                writer.close();
            }
        }
    }
}
