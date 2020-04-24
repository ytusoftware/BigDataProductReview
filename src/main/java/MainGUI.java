import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableRowSorter;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class MainGUI extends JFrame {
    private JPanel rootPanel;
    private JComboBox comboBox1;
    private JButton button1;
    private JTable table1;
    private JTextField searchProduct;
    private JButton searchButton;
    private JComboBox guiCategories;
    private JButton filterButton;
    private JTabbedPane tabbedPane1;
    private JPanel HDFSSec;
    private JPanel MapReduceSec;
    private JTable table2;
    private JButton listHDFSContentButton;
    private JButton deleteSelectedFileButton;
    private JButton downloadSelectedFileButton;
    private JButton addNewFileButton;
    private JLabel testLabel;
    private DefaultTableModel dtmMapReduce;
    private DefaultTableModel dtmHDFS;
    private TableRowSorter<DefaultTableModel> trs;
    private HashMap<String,Integer> productNameRowIndex;        /* This hash map holds table row index for each product name */
    private FileStatus[] files;                                 /* Array that holds the information of the files in the HDFS table. */
    private HDFSOperations hdfsOp;                              /* The class in which the operations on the file system are executed. */
    public MainGUI() {
        /* Adding the root panel */
        this.add(rootPanel);
        this.setTitle("Amazon Product Review Analysis");
        this.setSize(750, 500);
        this.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

        /* The file system is initializing. */
        hdfsOp = new HDFSOperations();
        try {
            /* Files in the file system are listed. */
            listFileStatus();
        }
        catch (IOException ex){
            ex.printStackTrace();
        }

        button1.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                MROperations mrOp = new MROperations();
                HashMap<String,Double> jobResults = new HashMap<String, Double>();

                switch(comboBox1.getSelectedIndex()) {
                    /* Minimum Reducer */
                    case 0:
                        mrOp.setStatisticalReducer(StatisticalReducer.MinReducer.class);
                        break;
                    /* Maximum Reducer */
                    case 1:
                        mrOp.setStatisticalReducer(StatisticalReducer.MaxReducer.class);
                        break;
                    /* Mean Reducer */
                    case 2:
                        mrOp.setStatisticalReducer(StatisticalReducer.MeanReducer.class);
                        break;
                    /* Std Dev Reducer */
                    case 3:
                        mrOp.setStatisticalReducer(StatisticalReducer.StdDevReducer.class);
                        break;
                    /* Mode Reducer */
                    case 4:
                        mrOp.setStatisticalReducer(StatisticalReducer.ModeReducer.class);
                        break;
                    /* Count Reducer */
                    case 5:
                        mrOp.setStatisticalReducer(StatisticalReducer.CountReducer.class);
                        break;

                    default:
                        // code block
                }

                long startTime = System.nanoTime();

                /* Running the user's desired statistical function */
                mrOp.runHadoopJob();

                long elapsedTime = System.nanoTime() - startTime;

                System.out.println("Total execution time in seconds: "+ elapsedTime/1000000000);


                try {
                    jobResults = mrOp.getResults();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }

                /* Inserting results to the table */
                insertResultsToTable(jobResults);

            }
        });
        searchButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                productNameFilter(searchProduct.getText());
            }
        });
        filterButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String pattern;

                pattern = "";

                if (guiCategories.getSelectedIndex() > 0) {
                    pattern = guiCategories.getItemAt(guiCategories.getSelectedIndex()).toString();
                    pattern += " \\| ";
                    System.out.println(pattern);
                }
                productCategoryFilter(pattern);
            }
        });
        listHDFSContentButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

                try {
                    /* Files in the file system are listed. */
                    listFileStatus();

                } catch (IOException ex) {
                    ex.printStackTrace();
                }

            }
        });
        deleteSelectedFileButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

                FileStatus file;
                int row;

                /* It is determined which line is selected. */
                row = table2.getSelectedRow();

                if ( row >= 0 ) {
                    file = files[row];

                    try{
                        /* The selected file is deleted from the file system. */
                        hdfsOp.deleteFile(file.getPath());
                        /* Current files are listed in the file system. */
                        listFileStatus();
                    }
                    catch (IOException ex){
                        ex.printStackTrace();
                    }

                }
                else {
                    /* If no file is selected, the user is informed with a warning message. */
                    JOptionPane.showMessageDialog(table2,"Please select the file to delete.","Warning",JOptionPane.WARNING_MESSAGE);
                }
            }
        });
        downloadSelectedFileButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

                FileStatus file;
                int row;

                /* It is determined which line is selected. */
                row = table2.getSelectedRow();

                if ( row >= 0 && files[row].isFile()){

                    file = files[row];

                    try{
                        /* The selected file is download from the file system. */
                        hdfsOp.downloadFile(file.getPath());
                        /* Current files are listed in the file system. */
                        listFileStatus();
                    }
                    catch (IOException ex){
                        ex.printStackTrace();
                    }

                }
                else{
                    /* If no file is selected, the user is informed with a warning message. */
                    JOptionPane.showMessageDialog(table2,"Please select the file to download.","Warning",JOptionPane.WARNING_MESSAGE);
                }

            }
        });
        addNewFileButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

                try{
                    /* The file on the user's computer is uploaded to the file system. */
                    hdfsOp.uploadFile();
                    /* Current files are listed in the file system. */
                    listFileStatus();
                }
                catch (IOException ex){
                    ex.printStackTrace();
                }

            }
        });
    }

    private void createUIComponents() {
        // TODO: place custom component creation code here

        /* Creating model for MapReduce Result Table */
        dtmMapReduce = new DefaultTableModel();
        dtmMapReduce.addColumn("Product Name");
        dtmMapReduce.addColumn("Min");
        dtmMapReduce.addColumn("Max");
        dtmMapReduce.addColumn("Mean");
        dtmMapReduce.addColumn("Std Dev");
        dtmMapReduce.addColumn("Mode");
        dtmMapReduce.addColumn("Count");

        /* Creating model for HDFS Table */
        dtmHDFS = new DefaultTableModel();
        dtmHDFS.addColumn("Name");
        dtmHDFS.addColumn("Size");
        dtmHDFS.addColumn("Replication");
        dtmHDFS.addColumn("Block Size");

        /* Creating row sorter for table */
        trs = new TableRowSorter<DefaultTableModel>(dtmMapReduce);

        table1 = new JTable(dtmMapReduce);
        table1.setRowSorter(trs);
        table1.setModel(dtmMapReduce);

        table2 = new JTable(dtmHDFS);
        table2.setModel(dtmHDFS);

    }

    /* Initialized the MapReduce result table when results are displayed for the first time */
    private void initResultTable(HashMap<String,Double> jobResults) {
        int currRowIndex = 0;
        Iterator iter = MROperations.productCategories.iterator();

        /* If there are no rows, adding the rows first. And also creating the product name row index map */
        if (dtmMapReduce.getRowCount() != jobResults.keySet().size()) {
            productNameRowIndex = new HashMap<String, Integer>();

            /* Adding empty rows */
            for (String productName : jobResults.keySet()) {
                dtmMapReduce.addRow(new Object[]{productName,"","","","","","" });
                productNameRowIndex.put(productName,currRowIndex);
                currRowIndex++;
            }

            /* Adding product categories to the combobox */
            while (iter.hasNext()) {
                guiCategories.addItem(iter.next().toString());
            }

            /* Displaying the info msg */
            JOptionPane.showMessageDialog(this.rootPanel,"A total of "+jobResults.keySet().size()+" products were detected and analyzed.");
        }
    }

    /* Inserts MapReduce job results to the JTable component */
    private void insertResultsToTable(HashMap<String,Double> jobResults) {
        int currRowIndex = 0;

        initResultTable(jobResults);


        /* Setting the selected reducer's results in the table */
        for(String productName : jobResults.keySet()) {
            currRowIndex = productNameRowIndex.get(productName);
            dtmMapReduce.setValueAt(jobResults.get(productName),currRowIndex,comboBox1.getSelectedIndex()+1);
        }

        /* Clearing the product category hashset */
        MROperations.productCategories.clear();

    }

    /* For search box filtering by product name */
    private void productNameFilter(String initialPattern) {
        RowFilter<DefaultTableModel, Object> rf = null;
        //If current expression doesn't parse, don't update.
        try {
            rf = RowFilter.regexFilter(initialPattern, 0);
        } catch (java.util.regex.PatternSyntaxException e) {
            System.err.println("parse error");
            return;
        }
        trs.setRowFilter(rf);

    }

    /* For product category filtering */
    private void productCategoryFilter(String initialPattern) {
        RowFilter<DefaultTableModel, Object> rf = null;
        //If current expression doesn't parse, don't update.
        try {
            rf = RowFilter.regexFilter("^"+initialPattern+"*", 0);
        } catch (java.util.regex.PatternSyntaxException e) {
            System.err.println("parse error");
            return;
        }
        trs.setRowFilter(rf);

    }

    private void listFileStatus() throws IOException{

        /* Getting the status of the files in dataset directory */
        files = hdfsOp.getHDFSContent();

        dtmHDFS.setRowCount(0);

        /* Adding the files as the rows of the HDFS jTable */
        for(FileStatus file:files) {

            dtmHDFS.addRow(new Object[]{file.getPath().getName(),FileUtils.byteCountToDisplaySize(file.getLen()) , file.getReplication(), FileUtils.byteCountToDisplaySize(file.getBlockSize())});
        }
    }

}
