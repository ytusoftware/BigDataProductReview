/*
 * This class contains code related to HDFS operations via GUI of the program.
 * Responsible: Onur Oztunc
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class HDFSOperations {

    private Configuration conf;             /* Used for Hadoop worker configuration */
    private String inputDirectoryPath;      /* The Amazon Dataset is in that directory */

    public HDFSOperations() {

        String nameNodeIp = "172.20.10.10"; //ONEMLI: Bunu kendi namenode ip adresin ile degistir Onur.

        /* Creating the configuration instance */
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://"+nameNodeIp+":9000");
        conf.set("mapreduce.jobtracker.address", nameNodeIp+":54311");

        /* Setting HDFS input directory path */
        inputDirectoryPath = "/customerReview/input";
    }

    /* Gets the files in the HDFS input directory */
    public FileStatus[] getHDFSContent() throws IOException {

        FileSystem fs;

        /* Creating file system instance then getting the status of the files */
        fs = FileSystem.get(URI.create(inputDirectoryPath), conf);
        FileStatus[] files = fs.listStatus(new Path(inputDirectoryPath));

        return files;
    }

}
