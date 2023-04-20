package com.example.uploadfile;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;


@RestController
public class Controller {

    @PostMapping("/uploadFile")
    public void uploadFile(@RequestParam("file") MultipartFile file) throws URISyntaxException, IOException {
        Configuration entries = new Configuration();
        entries.set("dfs.blocksize","218M");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), entries);

        InputStream in = new BufferedInputStream(file.getInputStream());
        Path path = new Path("/hdfsapi/test1/" + file.getOriginalFilename());
        FSDataOutputStream out = fileSystem.create(path);
        IOUtils.copy(in, out ,40960);
    }

    @PostMapping("/upload")
    public void upload() throws Exception {


        String name = "9-3【回放】Redis Cluster集群运维与核心原理剖析.mp4";

        MultiThreadUploadToHDFS.uploadFile("/Users/xiaohugg/baiduwp/" + name,"/hdfsapi/test/" + name ,name);
    }



}
