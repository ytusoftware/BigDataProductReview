/*
 * This class is responsible for starting the program and so the GUI.
 */

import javax.swing.*;

public class MainProgram {

    public static MainGUI guiForm;

    public static void main(String[] args) throws ClassNotFoundException, UnsupportedLookAndFeelException, InstantiationException, IllegalAccessException {
        /* Setting the GUI appearance */
        UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());

        /* Starting the GUI */
        MainProgram.guiForm = new MainGUI();
        MainProgram.guiForm.setVisible(true);
    }
}
