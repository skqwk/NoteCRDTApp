import org.sqlite.JDBC;

import javax.swing.*;
import javax.swing.event.TableModelListener;
import javax.swing.table.DefaultTableModel;
import javax.xml.transform.Result;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Main extends JFrame {

    private JTable table;
    private String[] columns = {"rowid", "timestamp", "data"};
    private TableModelListener tableModelListener;


    public Main() {
        DefaultTableModel model = new DefaultTableModel(columns, 0);
        table = new JTable(model);

        try {
            createSQL();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        try {
            populateSQL(table);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        table.setCellSelectionEnabled(true);
        table.setPreferredScrollableViewportSize(new Dimension(1200, 900));
        table.setFillsViewportHeight(true);

        JButton delete = new JButton("Delete");
        delete.addActionListener(e -> {  });
        JScrollPane scrollPane = new JScrollPane(table);
        scrollPane.setVisible(true);
        getContentPane().add(scrollPane, BorderLayout.PAGE_START);
    }

    public void createSQL() throws SQLException, ClassNotFoundException {
        Class.forName("org.sqlite.JDBC");

        Connection conn = DriverManager.getConnection("jdbc:sqlite:test.db");
        Statement stat = conn.createStatement();
        stat.executeUpdate("CREATE TABLE IF NOT EXISTS messages (timestamp, data)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO messages VALUES (?, ?);");

        prep.setString(1, "111");
        prep.setString(2, "aaa");
        prep.addBatch();

        prep.setString(1, "222");
        prep.setString(2, "bbb");
        prep.addBatch();

        prep.setString(1, "333");
        prep.setString(2, "ccc");
        prep.addBatch();

        conn.setAutoCommit(false);
        prep.executeBatch();
        conn.setAutoCommit(true);
        conn.close();
    }

    public void populateSQL(JTable table) throws ClassNotFoundException, SQLException {
        Class.forName("org.sqlite.JDBC");
        Connection conn = DriverManager.getConnection("jdbc:sqlite:test.db");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT rowId, * FROM messages");
        while (rs.next()) {
            Object[] row = new Object[columns.length];
            for (int i = 1; i <= columns.length; ++i) {
                row[i - 1] = rs.getObject(i);
            }

            ((DefaultTableModel) table.getModel()).insertRow(rs.getRow() - 1, row);
        }
        rs.close();
        conn.close();
    }

    public void deleteSQL(String rowId) throws ClassNotFoundException {
        Class.forName("org.sqlite.JDBC");
    }


    public static void main(String[] args) {
        Main frame = new Main();
        frame.setDefaultCloseOperation(EXIT_ON_CLOSE);
        frame.pack();
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);
    }
}
