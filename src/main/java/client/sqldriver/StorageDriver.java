package client.sqldriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class StorageDriver {

    public final String DB_NAME;
    public static final String TABLE_NAME = "storage";
    public final String URL;

    static {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException ex) {
            System.err.println(ex.getMessage());
            ex.printStackTrace();
        }
    }

    public StorageDriver(String nodeId) {
        this.DB_NAME = nodeId;
        this.URL = String.format("jdbc:sqlite:%s.db", DB_NAME);
        init();
    }

    private void init() {
        try {
            createTable();
        } catch (Exception ex) {
            System.err.println(ex.getMessage());

            ex.printStackTrace();
        }
    }

    private void createTable() {
        String query = String.format("CREATE TABLE IF NOT EXISTS %s(" +
                "key TEXT, " +
                "value TEXT, " +
                "PRIMARY KEY (key)" +
                ")", TABLE_NAME);

        try (Connection conn  = this.connect();
             Statement stat = conn.createStatement()) {
            stat.executeUpdate(query);
            System.out.println("Storage table is created");
        } catch (SQLException ex) {
            System.err.println(ex.getMessage());
            ex.printStackTrace();
        }
    }

    private Connection connect() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(URL);
        } catch (SQLException ex) {
            System.err.println(ex.getMessage());
            ex.printStackTrace();
        }
        return conn;
    }

    public void put(String key, Object value) {
        String query = String.format("INSERT OR REPLACE INTO %s (" +
                "key, " +
                "value) " +
                "VALUES(?,?)", TABLE_NAME);
        try(Connection conn = this.connect();
            PreparedStatement prep = conn.prepareStatement(query)) {
            prep.setString(1, key);
            prep.setObject(2, value);
            prep.execute();
        } catch (SQLException ex) {
            ex.printStackTrace();
            System.err.println(ex.getMessage());
        }
    }

    public Object get(String key) {
        String query = String.format(
                "SELECT value " +
                        "FROM %s " +
                        "WHERE key = '%s'",
                TABLE_NAME,
                key);
        Object value = null;
        try(Connection conn = this.connect();
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(query)) {
            if (rs.next()) {
                value = rs.getObject("value");
            }
        } catch (Exception ex
        ) {
            ex.printStackTrace();
            System.err.println(ex.getMessage());
        }

        return value;
    }

}
