package com.example.uploadfile;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import static com.example.uploadfile.MultiThreadUploadToHDFS.THREAD_POOL_SIZE;

public class MultiThreadUploadToHDFS {
    private static final int BLOCK_SIZE = 1024 * 1024 * 1024; // 每个块的大小
    public static final int THREAD_POOL_SIZE = 5; // 合并线程池大小

    public static void uploadFile(String localFilePath, String hdfsFilePath,String name) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);

        File file = new File(localFilePath);
        long fileSize = file.length();
        int blockNum = (int) Math.ceil((double) fileSize / BLOCK_SIZE);


        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        String tempFile = hdfsFilePath.replaceFirst(name, "");
        tempFile = tempFile + UUID.randomUUID();
        Path path = new Path(tempFile);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
        CountDownLatch countDownLatch = new CountDownLatch(blockNum);
        List<String> blockFiles = new ArrayList<>();
        // 分块上传
        for (int i = 0; i < blockNum; i++) {
            int blockIndex = i;
            String finalTempFile = tempFile;
            executorService.execute(() -> {

                try  {
                    FileSystem fs1 = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
                    byte[] buffer = new byte[BLOCK_SIZE];
                    InputStream in = Files.newInputStream(file.toPath());
                    in.skip((long) blockIndex * BLOCK_SIZE);
                    int length = in.read(buffer);


                    Path hdfsPath = new Path(finalTempFile +"/" + name+ ".part_" + blockIndex);
                    if (fs1.exists(hdfsPath)) {
                        fs1.delete(hdfsPath, true);
                    }

                    fs1.createNewFile(hdfsPath);

                    FSDataOutputStream out = fs1.append(hdfsPath);
                    out.write(buffer, 0, length);
                    out.close();
                    in.close();


                    blockFiles.add(hdfsPath.toString());

                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {

                }
            });
        }
        countDownLatch.await();
        fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
        mergeFiles(blockFiles,hdfsFilePath,fs,tempFile);
    }

    public static void mergeFiles(List<String> blockFiles, String outputPath,FileSystem fs,String inputFilePath) throws IOException {


        // 打开输出文件的输出流
        Path outPath = new Path(outputPath);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true); // 如果输出文件已经存在，则删除
        }
        FSDataOutputStream out = fs.create(outPath);

        // 遍历输入路径下的所有小文件，并将它们的内容写入输出流中
        Collections.sort(blockFiles);
        for (String fileName : blockFiles) {
            Path filePath = new Path(fileName);
            FSDataInputStream in = fs.open(filePath);
            byte[] buffer = new byte[4096];
            int bytesRead = 0;
            while ((bytesRead = in.read(buffer)) > 0) {
                out.write(buffer, 0, bytesRead);
            }
            in.close();
        }
        fs.delete(new Path(inputFilePath),true);
        out.close();
        fs.close();
    }
}

class MultiThreadFileMerge {

    private static final int BLOCK_SIZE = 128 * 1024 * 1024; // 每个块的大小

    public static void merge(List<String> blockFiles, String mergeFilePath) throws IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        List<InputStream> inputStreamList = new ArrayList<>(blockFiles.size());
        for (String blockFile : blockFiles) {
            inputStreamList.add(new BufferedInputStream(new FileInputStream(blockFile)));
        }

        // 多线程合并块文件
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(mergeFilePath))) {
            long totalSize = 0;
            byte[] buffer = new byte[BLOCK_SIZE];
            int bytesRead = 0;
            while ((bytesRead = inputStreamList.get(0).read(buffer)) > 0) {
                if (totalSize >= BLOCK_SIZE) {
                    break;
                }
                out.write(buffer, 0, bytesRead);
                totalSize += bytesRead;
            }

            while (totalSize < BLOCK_SIZE) {
                for (int i = 1; i < inputStreamList.size(); i++) {
                    InputStream in = inputStreamList.get(i);
                    bytesRead = in.read(buffer);
                    if (bytesRead > 0) {
                        // 写入合并文件
                        out.write(buffer, 0, bytesRead);
                        totalSize += bytesRead;
                    }
                }
            }

            // 关闭输入流
            for (InputStream in : inputStreamList) {
                in.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        executorService.shutdown();
    }
}

