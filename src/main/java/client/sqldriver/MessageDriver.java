package client.sqldriver;

import client.hlc.HLC;
import client.hlc.HybridTimestamp;
import client.util.ParserValue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MessageDriver {

    public final String DB_NAME;
    public static final String TABLE_NAME = "messages";
    public final String URL;

    private final HLC clock;

    static {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException ex) {
            System.err.println(ex.getMessage());
            ex.printStackTrace();
        }
    }

    public MessageDriver(HLC clock) {
        this.DB_NAME = clock.getNodeId();
        this.URL = String.format("jdbc:sqlite:%s.db", DB_NAME);
        this.clock = clock;
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
                "timestamp TEXT, " +
                "entity_type TEXT, " +
                "entity_id INTEGER, " +
                "field TEXT, " +
                "field_type TEXT, " +
                "field_value TEXT, " +
                "is_not_deleted INTEGER DEFAULT '1' NOT NULL, " +
                "PRIMARY KEY (entity_type, entity_id, field)" +
                ")", TABLE_NAME);

        try (Connection conn  = this.connect();
             Statement stat = conn.createStatement()) {
            stat.executeUpdate(query);
            System.out.println("Table is created");
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

    public <T> T read(Long id, Class<T> cl) {
        String query = String.format(
                "SELECT timestamp, entity_type, entity_id, field, field_type, field_value, is_not_deleted " +
                        "FROM %s " +
                        "WHERE entity_type = '%s' " +
                        "AND entity_id = %s",
                TABLE_NAME,
                cl.getSimpleName(),
                id);
        T object = null;
        try(Connection conn = this.connect();
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(query)) {

            List<Map<String, Object>> rows = selectRows(rs);

            int fields = cl.getDeclaredFields().length - 1;
            List<Map<String, Object>> objectFields = sortAndSlice(rows, fields);
            object = fillObjectFields(objectFields, cl, id);

        } catch (Exception ex
        ) {
            ex.printStackTrace();
            System.err.println(ex.getMessage());
        }

        return object;

    }

    private <T> T fillObjectFields(List<Map<String, Object>> objectFields,
                                      Class<T> cl, Long objectId
    ) throws Exception {
        Object builder = cl.getDeclaredMethod("builder").invoke(null);
        T object = (T) builder.getClass().getDeclaredMethod("build").invoke(builder);
        Field id = cl.getDeclaredField("id");
        id.setAccessible(true);
        id.set(object, objectId);
        for (Map<String, Object> objectField : objectFields) {
            Field field = cl.getDeclaredField(objectField.get("field").toString());
            field.setAccessible(true);
            Object value = ParserValue.valueOf(
                    objectField.get("fieldType").toString())
                    .parse.apply(objectField.get("fieldValue").toString());

            field.set(object, value);
        }

        return object;


    }


    public void write(Object o, Class<?> cl) {
        String query = String.format("INSERT OR REPLACE INTO %s (" +
                "timestamp, " +
                "entity_type, " +
                "entity_id, " +
                "field, " +
                "field_type, " +
                "field_value) " +
                "VALUES(?,?,?,?,?,?)", TABLE_NAME);

        try(Connection conn = this.connect();
            PreparedStatement prep = conn.prepareStatement(query)) {
            for (Field field : cl.getDeclaredFields()) {
                if (!field.getName().equals("id")) {
                    field.setAccessible(true);
                    prep.setString(1, clock.now().toString());
                    prep.setString(2, cl.getSimpleName());
                    prep.setLong(3, (Long) cl.getDeclaredMethod("getId").invoke(o));
                    prep.setString(4, field.getName());
                    prep.setString(5, field.getType().getSimpleName().toUpperCase());
                    prep.setObject(6, field.get(o));
                    prep.addBatch();
                }
            }
            conn.setAutoCommit(false);
            prep.executeBatch();
            conn.setAutoCommit(true);
            System.out.println("Object is written");
        } catch (SQLException
                | NoSuchMethodException
                | IllegalAccessException
                | InvocationTargetException ex) {
            ex.printStackTrace();
            System.err.println(ex.getMessage());
        }

    }

    public <T> List<T> readAll(Class<T> cl) {
        String query = String.format(
                "SELECT timestamp, entity_type, entity_id, field, field_type, field_value " +
                        "FROM %s " +
                        "WHERE entity_type = '%s'" +
                        "AND is_not_deleted ",
                TABLE_NAME,
                cl.getSimpleName());
        List<T> objects = new ArrayList<>();
        try(Connection conn = this.connect();
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(query)) {

            // Выбираем все строчки, относящиеся к данному типу сущности
            List<Map<String, Object>> rows = selectRows(rs);

            // Разделяем - относим строчки к id сущности
            Map<Long, List<Map<String, Object>>> idRows = new HashMap<>();
            for (Map<String, Object> row : rows) {
                Long entityId = Long.parseLong(row.get("entityId").toString());
                if (idRows.containsKey(entityId)) {
                    idRows.get(entityId).add(row);
                } else {
                    idRows.put(entityId, new ArrayList<>(List.of(row)));
                }
            }

            int fields = cl.getDeclaredFields().length - 1;
            for (Long id : idRows.keySet()) {
                List<Map<String, Object>> objectFields = sortAndSlice(idRows.get(id), fields);
                objects.add(fillObjectFields(objectFields, cl, id));
            }


        } catch (Exception ex
        ) {
            ex.printStackTrace();
            System.err.println(ex.getMessage());
        }

        return objects;
    }

    private List<Map<String, Object>> sortAndSlice(List<Map<String, Object>> rows, int fields) {
        return rows.stream().sorted((row1, row2) -> {
                    String t1 = row1.get("timestamp").toString();
                    String t2 = row2.get("timestamp").toString();
                    return HybridTimestamp.parse(t1).compareTo(HybridTimestamp.parse(t2));
                })
//                .skip(rows.size() - fields)
                .collect(Collectors.toList());
    }

    private List<Map<String, Object>> selectRows(ResultSet rs) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();

            row.put("timestamp", rs.getObject("timestamp"));
            row.put("entityType", rs.getObject("entity_type"));
            row.put("entityId", rs.getObject("entity_id"));
            row.put("field", rs.getObject("field"));
            row.put("fieldType", rs.getObject("field_type"));
            row.put("fieldValue", rs.getObject("field_value"));

            if (hasColumn(rs, "is_not_deleted")) {
                row.put("isNotDeleted", rs.getObject("is_not_deleted"));
            }

            rows.add(row);
        }

        return rows;
    }

    public static boolean hasColumn(ResultSet rs, String columnName) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columns = rsmd.getColumnCount();
        for (int x = 1; x <= columns; x++) {
            if (columnName.equals(rsmd.getColumnName(x))) {
                return true;
            }
        }
        return false;
    }

    public <T> void markAsDeleted(Long id, Class<T> cl)  {
        String query = String.format("UPDATE %s " +
                "SET is_not_deleted = '0', " +
                "timestamp = '%s' " +
                "WHERE entity_type = '%s'" +
                "AND entity_id = '%s'",
                TABLE_NAME,
                clock.now(),
                cl.getSimpleName(),
                id);

        try(Connection conn = this.connect();
            Statement stat = conn.createStatement()) {
            stat.executeUpdate(query);
            System.out.println("Object is deleted");
        } catch (SQLException ex) {
            ex.printStackTrace();
            System.err.println(ex.getMessage());
        }

    }

    public  <T> void replace(Long id, Map<String, Object> updatedFields, Class<T> cl) {
        String query = String.format("INSERT OR REPLACE INTO %s (" +
                "timestamp, " +
                "entity_type, " +
                "entity_id, " +
                "field, " +
                "field_type, " +
                "field_value) " +
                "VALUES(?,?,?,?,?,?)", TABLE_NAME);

        try(Connection conn = this.connect();
            PreparedStatement prep = conn.prepareStatement(query)) {
            for (String fieldName : updatedFields.keySet()) {
                    prep.setString(1, clock.now().toString());
                    prep.setString(2, cl.getSimpleName());
                    prep.setLong(3, id);
                    prep.setString(4, fieldName);
                    prep.setString(5, cl.getDeclaredField(fieldName).getType().getSimpleName().toUpperCase());
                    prep.setObject(6, updatedFields.get(fieldName));
                    prep.addBatch();

            }
            conn.setAutoCommit(false);
            prep.executeBatch();
            conn.setAutoCommit(true);
        } catch (SQLException
                | NoSuchFieldException ex) {
            ex.printStackTrace();
            System.err.println(ex.getMessage());
        }
    }

    public <T> List<Map<String, Object>> readAllSince(HybridTimestamp since) {
        String query = String.format(
                "SELECT timestamp, entity_type, entity_id, field, field_type, field_value, is_not_deleted " +
                        "FROM %s " +
                        "WHERE timestamp >='%s' ",
                TABLE_NAME,
                since);
        List<Map<String, Object>> rows = new ArrayList<>();
        try(Connection conn = this.connect();
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(query)) {
            rows = selectRows(rs);
        } catch (Exception ex
        ) {
            ex.printStackTrace();
            System.err.println(ex.getMessage());
        }

        return rows;
    }

    public void merge(List<Map<String, Object>> remoteMessages) {
        String query = String.format("INSERT INTO %s (" +
                "timestamp, " +
                "entity_type, " +
                "entity_id, " +
                "field, " +
                "field_type, " +
                "field_value," +
                "is_not_deleted) " +
                "VALUES(?,?,?,?,?,?,?) " +
                "ON CONFLICT (entity_type, entity_id, field) DO UPDATE " +
                "SET field_value = excluded.field_value, timestamp = excluded.timestamp, " +
                "is_not_deleted = excluded.is_not_deleted " +
                "WHERE excluded.timestamp >= %s.timestamp"
                , TABLE_NAME, TABLE_NAME);

        try(Connection conn = this.connect();
            PreparedStatement prep = conn.prepareStatement(query)) {
            for (Map<String, Object> message : remoteMessages) {
                    clock.tick(HybridTimestamp.parse(message.get("timestamp").toString()));
                    prep.setString(1, message.get("timestamp").toString());
                    prep.setString(2, message.get("entityType").toString());
                    prep.setLong(3, (Long) message.get("entityId"));
                    prep.setString(4, message.get("field").toString());
                    prep.setString(5, message.get("fieldType").toString());
                    prep.setObject(6, message.get("fieldValue"));
                    prep.setObject(7, message.get("isNotDeleted"));
                    prep.addBatch();
            }
            conn.setAutoCommit(false);
            prep.executeBatch();
            conn.setAutoCommit(true);
        } catch (SQLException ex) {
            ex.printStackTrace();
            System.err.println(ex.getMessage());
        }
    }

    public  <T> void writeMessages(List<Map<String, Object>> messages) {
        String query = String.format("INSERT OR REPLACE INTO %s (" +
                "timestamp, " +
                "entity_type, " +
                "entity_id, " +
                "field, " +
                "field_type, " +
                "field_value, " +
                "is_not_deleted)" +
                "VALUES(?,?,?,?,?,?,?)", TABLE_NAME);

        try(Connection conn = this.connect();
            PreparedStatement prep = conn.prepareStatement(query)) {
            for (Map<String, Object> message : messages) {
                clock.tick(HybridTimestamp.parse(message.get("timestamp").toString()));

                prep.setString(1, message.get("timestamp").toString());
                prep.setString(2, message.get("entityType").toString());
                prep.setLong(3, (Long) message.get("entityId"));
                prep.setString(4, message.get("field").toString());
                prep.setString(5, message.get("fieldType").toString());
                prep.setObject(6, message.get("fieldValue"));
                prep.setObject(7, message.get("isNotDeleted"));
                prep.addBatch();
            }
            conn.setAutoCommit(false);
            prep.executeBatch();
            conn.setAutoCommit(true);
        } catch (SQLException ex) {
            ex.printStackTrace();
            System.err.println(ex.getMessage());
        }
    }

    public List<Map<String, Object>> readAllMessages() {
        String query = String.format(
                "SELECT timestamp, entity_type, entity_id, field, field_type, field_value, is_not_deleted " +
                        "FROM %s ",
                TABLE_NAME);
        List<Map<String, Object>> rows = new ArrayList<>();
        try(Connection conn = this.connect();
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(query)) {
            rows = selectRows(rs);
        } catch (Exception ex
        ) {
            ex.printStackTrace();
            System.err.println(ex.getMessage());
        }

        return rows;
    }
}
