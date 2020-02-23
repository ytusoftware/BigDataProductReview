import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableColumn;
import javax.swing.table.TableModel;
import javax.xml.bind.annotation.XmlType;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.HashMap;

public class MainGUI extends JFrame {
    private JPanel rootPanel;
    private JComboBox comboBox1;
    private JButton button1;
    private JTable table1;
    private JLabel testLabel;
    private DefaultTableModel dtm;

    public MainGUI() {
        /* Adding the root panel */
        this.add(rootPanel);
        this.setTitle("Amazon Product Review Analysis");
        this.setSize(400, 500);
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
                    default:
                        // code block
                }

                /* Before inserting results, clearing the table */
                dtm.setRowCount(0);
                /* Running the user's desired statistical function */
                mrOp.runHadoopJob();
                try {
                    jobResults = mrOp.getResults();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
                insertResultsToTable(jobResults);

            }
        });
    }

    private void createUIComponents() {
        // TODO: place custom component creation code here

        /* Creating model for table */
        dtm = new DefaultTableModel();
        dtm.addColumn("Product Name");
        dtm.addColumn("Result");
        table1 = new JTable(dtm);
    }

    /* Inserts MapReduce job results to the JTable component */
    private void insertResultsToTable(HashMap<String,Double> jobResults) {

        for(String productName : jobResults.keySet()) {
            dtm.addRow(new Object[]{productName, jobResults.get(productName)});
        }

    }

}
