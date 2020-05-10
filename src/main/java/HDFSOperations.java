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
import java.net.InetAddress;
import java.net.URI;

enum RETURN_VAL{
    PATH_EXIST, FILE_EXIST, PATH_INVALID, NO_ERROR;
}

public class HDFSOperations {

    private Configuration conf;             /* Used for Hadoop worker configuration */
    //private String inputDirectoryPath;      /* The Amazon Dataset is in that directory */
    private Path currentPath;               /* Current directory path information is kept. */
    private FileSystem fs;

    public HDFSOperations() {

        String nameNodeIp = "localhost"; //ONEMLI: Bunu kendi namenode ip adresin ile degistir Onur.
        try{
            InetAddress inetAddress = InetAddress.getLocalHost();
            nameNodeIp = inetAddress.getHostAddress();
        }
        catch (Exception ex){
            ex.printStackTrace();
        }

        /* Creating the configuration instance */
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://"+nameNodeIp+":9000");
        conf.set("mapreduce.jobtracker.address", nameNodeIp+":54311");

        /* Setting HDFS input directory path */
        //inputDirectoryPath = "/customerReview/input";
        currentPath = new Path("/");

        try{
            /* Creating file system */
            fs = FileSystem.get(conf);
        }
        catch (IOException ex){
            ex.printStackTrace();
        }

    }

    public Path getCurrentPath() {
        return currentPath;
    }

    public void setCurrentPath(Path currentPath) {
        this.currentPath = currentPath;
    }

    /* Gets the files in the HDFS input directory */
    public FileStatus[] getHDFSContent() throws IOException {

        /* Getting the status of the files in file system */
        FileStatus[] files = fs.listStatus(currentPath);

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
            fs.copyFromLocalFile(new Path(localPath), currentPath);
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

    /* The current directory of the file system is being changed. */
    public boolean goDirectory(Path destinationPath){

        /* If the isDir is true, destinationPath is a directory, otherwise it is not a directory. */
        boolean isDir = false;
        FileStatus fileStatus;

        try{
            isDir = fs.exists(destinationPath);

            /* Checking if path is valid. */
            if ( true == isDir ) {

                fileStatus = fs.getFileStatus(destinationPath);
                isDir = fileStatus.isDirectory();

                /* If the destination path is directory, the current system of the file system is changed. */
                if (true == isDir) {

                    currentPath = destinationPath;
                }
            }

        }
        catch (IOException ex){
            ex.printStackTrace();
        }

        return isDir;
    }

    /* The new directory entered by the user is created in the file-system. */
    public RETURN_VAL createDirectory(String newDirectory ){

        String newPath;
        boolean isPathExist;
        boolean isCreateDirectory;
        RETURN_VAL retVal = RETURN_VAL.NO_ERROR;

        /* The current directory and the directory you want to create new are combined. */
        if ( 0 == currentPath.toString().compareTo("/") ){
            newPath = currentPath.toString() + newDirectory;
        }
        else {
            newPath = currentPath.toString() + "/" + newDirectory;
        }

        /* Checking if the same directory exists. */
        try{
            isPathExist = fs.exists(new Path(newPath));

            if ( isPathExist ){
                retVal = RETURN_VAL.PATH_EXIST;
            }

        }
        catch (Exception ex){
            ex.printStackTrace();
            retVal = RETURN_VAL.PATH_INVALID;
        }

        /* Creating new directory. */
        try{

            if ( retVal == RETURN_VAL.NO_ERROR ){
                isCreateDirectory = fs.mkdirs(new Path(newPath));
                if ( !isCreateDirectory )
                {
                    retVal = RETURN_VAL.PATH_INVALID;
                }
            }

        }
        catch (Exception ex)
        {
            retVal = RETURN_VAL.PATH_INVALID;
            ex.printStackTrace();
        }

        return retVal;
    }


}
