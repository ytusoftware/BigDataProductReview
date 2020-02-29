import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableRowSorter;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.HashMap;

public class MainGUI extends JFrame {
    private JPanel rootPanel;
    private JComboBox comboBox1;
    private JButton button1;
    private JTable table1;
    private JTextField searchProduct;
    private JButton searchButton;
    private JLabel testLabel;
    private DefaultTableModel dtm;
    private TableRowSorter<DefaultTableModel> trs;
    private HashMap<String,Integer> productNameRowIndex;        /* This hash map holds table row index for each product name */

    public MainGUI() {
        /* Adding the root panel */
        this.add(rootPanel);
        this.setTitle("Amazon Product Review Analysis");
        this.setSize(550, 500);
        this.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);


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


                /* Running the user's desired statistical function */
                mrOp.runHadoopJob();

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
                productNameFilter(searchProduct);
            }
        });
    }

    private void createUIComponents() {
        // TODO: place custom component creation code here

        /* Creating model for table */
        dtm = new DefaultTableModel();
        dtm.addColumn("Product Name");
        dtm.addColumn("Min");
        dtm.addColumn("Max");
        dtm.addColumn("Mean");
        dtm.addColumn("Std Dev");
        dtm.addColumn("Mode");
        dtm.addColumn("Count");

        /* Creating row sorter for table */
        trs = new TableRowSorter<DefaultTableModel>(dtm);

        table1 = new JTable(dtm);
        table1.setRowSorter(trs);
        table1.setModel(dtm);

    }

    /* Inserts MapReduce job results to the JTable component */
    private void insertResultsToTable(HashMap<String,Double> jobResults) {
        int currRowIndex = 0;

        /* If there are no rows, adding the rows first. And also creating the product name row index map */
        if (dtm.getRowCount() != jobResults.keySet().size()) {
            productNameRowIndex = new HashMap<String, Integer>();

            for (String productName : jobResults.keySet()) {
                dtm.addRow(new Object[]{productName,"","","","","","" });
                productNameRowIndex.put(productName,currRowIndex);
                currRowIndex++;
            }

            JOptionPane.showMessageDialog(this.rootPanel,"A total of "+jobResults.keySet().size()+" products were detected and analyzed.");
        }


        /* Setting the selected reducer's results in the table */
        for(String productName : jobResults.keySet()) {
            currRowIndex = productNameRowIndex.get(productName);
            dtm.setValueAt(jobResults.get(productName),currRowIndex,comboBox1.getSelectedIndex()+1);
        }

    }

    /* For search box filtering by product name */
    private void productNameFilter(JTextField initial_box) {
        RowFilter<DefaultTableModel, Object> rf = null;
        System.out.println(initial_box.getText());
        //If current expression doesn't parse, don't update.
        try {
            rf = RowFilter.regexFilter(initial_box.getText(), 0);
        } catch (java.util.regex.PatternSyntaxException e) {
            System.err.println("parse error");
            return;
        }
        trs.setRowFilter(rf);

    }

}
