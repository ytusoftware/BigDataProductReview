/*
 * This class contains code related to HDFS operations via GUI of the program.
 * Responsible: Onur Oztunc
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.net.URI;

public class HDFSOperations {

    private Configuration conf;             /* Used for Hadoop worker configuration */
    private String inputDirectoryPath;      /* The Amazon Dataset is in that directory */
    private FileSystem fs;

    public HDFSOperations() {

        String nameNodeIp = "localhost"; //ONEMLI: Bunu kendi namenode ip adresin ile degistir Onur.

        /* Creating the configuration instance */
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://"+nameNodeIp+":9000");
        conf.set("mapreduce.jobtracker.address", nameNodeIp+":54311");

        /* Setting HDFS input directory path */
        //inputDirectoryPath = "/customerReview/input";
        inputDirectoryPath = "/bigDataProject";

        try{
            /* Creating file system */
            fs = FileSystem.get(URI.create(inputDirectoryPath), conf);
        }
        catch (IOException ex){
            ex.printStackTrace();
        }

    }

    /* Gets the files in the HDFS input directory */
    public FileStatus[] getHDFSContent() throws IOException {

        /* Getting the status of the files in file system */
        FileStatus[] files = fs.listStatus(new Path(inputDirectoryPath));

        return files;
    }

    /* The file is copied to the location specified by the user. */
    public void  downloadFile(Path filePath) throws  IOException {

        String localPath;       /* The path the user wants to copy the file to. */

        /* Initializing the JChooser object to be used by the user to select the path. */
        JFrame parentFrame = new JFrame();       /* Parent component of the dialog */
        final JFileChooser fileChooser = new JFileChooser();

        fileChooser.setDialogTitle("Choosing Location");
        fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);   /* Only directory can choose. */

        int userSelection = fileChooser.showOpenDialog(parentFrame);
        if (userSelection == JFileChooser.APPROVE_OPTION) {
            /* Retrieving path information chosen by the user. */
            File fileToSave = fileChooser.getSelectedFile();
            localPath = fileToSave.getAbsolutePath();

            /* The file is copied from the filesystem to the user's computer. */
            fs.copyToLocalFile(filePath, new Path(localPath));

        }
        else{
            System.out.println("The file download operation was cancelled. ");
        }

    }

    /* New file is copied from the user's computer to the file system. */
    public void uploadFile() throws IOException{

        String localPath;           /* The path of the file that the user wants to copy to the file system */

        /* Initializing the JChooser object to be used by the user to select the path. */
        JFrame parentFrame = new JFrame();      /* Parent component of the dialog */
        final JFileChooser fileChooser = new JFileChooser();

        fileChooser.setDialogTitle("Choosing File");
        fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);     /* Only directory can choose. */

        int userSelection = fileChooser.showOpenDialog(parentFrame);
        if (userSelection == JFileChooser.APPROVE_OPTION) {
            /* Retrieving file information chosen by the user. */
            File fileToSave = fileChooser.getSelectedFile();
            localPath = fileToSave.getAbsolutePath();

            /* The file is copied from the user's computer to the filesystem. */
            fs.copyFromLocalFile(new Path(localPath), new Path(inputDirectoryPath));
        }
        else{
            System.out.println("The file upload operation was cancelled. ");
        }
    }

    /* The file on the path is deleted from the file system. */
    public void deleteFile(Path filePath) throws IOException{

        /* The file on the path is deleted from the file system. */
        fs.delete(filePath, true);
    }

}
