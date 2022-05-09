package client.mvc.view;

import client.mvc.model.Record;
import client.mvc.service.RecordService;
import client.mvc.service.Service;
import client.sync.SyncService;

import javax.swing.*;
import javax.swing.event.ListSelectionEvent;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class ClientView extends JFrame {

    private Map<String, JTextField> fields = new HashMap<>();
    private Service<Record> recordService;
    private SyncService syncService;
    private DefaultTableModel model;
    private JTable table;
    private JButton save = new JButton("Save");
    private JButton update = new JButton("Update");
    private JButton delete = new JButton("Delete");
    private JButton sync = new JButton("Sync");
    private JButton clearSelection = new JButton("Clear select");
    private Long selectedIdEntity;
    private Map<String, String> selectedEntity = new HashMap<>();
    private GridBagLayout gridBag = new GridBagLayout();
    private JPanel pane = new JPanel();



    public ClientView(String nodeId, SyncService syncService, RecordService recordService) {
        super(nodeId);
        this.syncService = syncService;
        this.recordService = recordService;
    }

    public void run() {
        pane.setLayout(gridBag);
        configureForm();

        GridBagConstraints c = new GridBagConstraints();
        configureLayout(c);

        List<Field> declaredFields = java.util.List.of(Record.class.getDeclaredFields());
        model = new DefaultTableModel(declaredFields.stream()
                .map(Field::getName).toArray(), 0);
        fillModel();
        configureTable();

        configureTableLayout(c);
        getContentPane().add(pane, BorderLayout.CENTER);

        setDefaultCloseOperation(EXIT_ON_CLOSE);
        pack();
        setLocationRelativeTo(null);
        setVisible(true);
    }

    private void configureTableLayout(GridBagConstraints c ) {
        JScrollPane scrollPane = new JScrollPane(table);
        scrollPane.setVisible(true);
        c.weightx = 1;
        c.weighty = 1;
        c.gridx = 2;
        c.gridy = 0;
        c.gridheight = 9;
        c.fill = GridBagConstraints.BOTH;
        c.insets = new Insets(0, 10, 0,5);
        gridBag.setConstraints(scrollPane, c);
        pane.add(scrollPane);

        // Configure SYNC button
        sync.addActionListener(this::sync);
        c.gridx = 2;
        c.gridy = 9;
        c.gridheight = GridBagConstraints.REMAINDER;
        c.insets = new Insets(5, 10, 20, 5);
        c.fill = GridBagConstraints.HORIZONTAL;
        gridBag.setConstraints(sync, c);
        sync.setVisible(true);
        pane.add(sync);
    }

    private void configureLayout(GridBagConstraints c) {

        // Configure SAVE button
        c.gridx = 0;
        c.fill = GridBagConstraints.HORIZONTAL;
        save.addActionListener(this::save);
        c.gridy = 9;
        c.gridwidth = 2;
        c.insets = new Insets(5, 5, 20, 0);
        c.anchor = GridBagConstraints.NORTHWEST;
        gridBag.setConstraints(save, c);
        pane.add(save);

        // Configure UPDATE button
        update.addActionListener(this::update);
        c.gridy = c.gridy + 1;
        c.gridwidth = 1;
        gridBag.setConstraints(update, c);
        pane.add(update);
        update.setVisible(false);

        // Configure DELETE button
        delete.addActionListener(this::delete);
        c.gridx = 1;
        gridBag.setConstraints(delete, c);
        pane.add(delete);
        delete.setVisible(false);

        // Configure CLEAR_SELECTION button
        clearSelection.addActionListener(this::clearSelection);
        c.gridy = c.gridy + 1;
        c.gridwidth = 2;
        c.gridx = 0;
        gridBag.setConstraints(clearSelection, c);
        pane.add(clearSelection);
        clearSelection.setVisible(false);
    }

    private  void sync(ActionEvent actionEvent) {
        syncService.send();
        refreshTable();
        updateDeleteModeEnable(false);
    }

    private  void clearSelection(ActionEvent actionEvent) {
        table.clearSelection();
        updateDeleteModeEnable(false);
        clearForm();
    }

    private  void delete(ActionEvent actionEvent) {
        recordService.delete(selectedIdEntity);
        refreshTable();
        updateDeleteModeEnable(false);
        clearForm();
    }

    private  void refreshTable() {
        table.clearSelection();
        clearModel();
        fillModel();
        selectedEntity.clear();
    }

    private  void update(ActionEvent actionEvent) {

        Map<String, Object> updatedFields = new HashMap<>();
        for (String fieldName : selectedEntity.keySet()) {
            if (!fields.get(fieldName).getText().equals(selectedEntity.get(fieldName))) {
                updatedFields.put(fieldName, fields.get(fieldName).getText());
            }
        }

        System.out.println(updatedFields);

        recordService.update(selectedIdEntity, updatedFields);
        refreshTable();
        updateDeleteModeEnable(false);
        clearForm();
    }

    private  void clearForm() {
        for (String fieldName : fields.keySet()) {
            fields.get(fieldName).setText("");
        }
    }

    private  void configureTable() {
        table = new JTable() {
            public boolean editCellAt(int row, int column, java.util.EventObject e) {
                return false;
            }
        };

        table.setModel(model);
        table.removeEditor();
        table.setCellSelectionEnabled(false);
        table.setRowSelectionAllowed(true);
        table.getSelectionModel().addListSelectionListener(this::selectRowTable);

    }

    private  void selectRowTable(ListSelectionEvent listSelectionEvent) {
        int selectedRowIndex = table.getSelectedRow();
        int columnCount = table.getColumnCount();
        if (selectedRowIndex > -1) {
            for (int i = 0; i < columnCount; i++) {
                Object value = table.getModel().getValueAt(selectedRowIndex, i);
                String name = table.getModel().getColumnName(i);
                if (!name.equals("id")) {
                    selectedEntity.put(name, value.toString());
                    fields.get(name).setText(value.toString());
                } else {
                    selectedIdEntity = Long.parseLong(value.toString());
                }
            }
        }
        updateDeleteModeEnable(true);
    }

    private  void updateDeleteModeEnable(Boolean isEnable) {
        save.setVisible(!isEnable);
        update.setVisible(isEnable);
        delete.setVisible(isEnable);
        clearSelection.setVisible(isEnable);
    }

    private  void configureForm() {
        java.util.List<Field> declaredFields = java.util.List.of(Record.class.getDeclaredFields());
        List<String> fieldNames = declaredFields.stream()
                .map(Field::getName)
                .filter(name -> !name.equals("id"))
                .collect(Collectors.toList());

        GridBagConstraints c = new GridBagConstraints();
        c.gridy = 0;
        c.gridx = 0;
        c.fill = GridBagConstraints.HORIZONTAL;
        for (String fieldName : fieldNames) {
            JLabel label = new JLabel(fieldName);
            c.ipady = 20;
            c.gridwidth = 2;
            c.gridy = c.gridy + 1;
            c.insets = new Insets(5, 5, 0, 0);
            c.anchor = GridBagConstraints.NORTHWEST;
            gridBag.setConstraints(label, c);
            pane.add(label);

            JTextField field = new JTextField(10);
            field.setName(fieldName);
            fields.put(fieldName, field);
            c.ipadx = 40;
            c.gridwidth = 2;
            c.weightx = 0.1;
            c.gridy = c.gridy + 1;
            c.anchor = GridBagConstraints.NORTHWEST;
            c.insets = new Insets(0, 5, 0,0);
            gridBag.setConstraints(field, c);
            pane.add(field);
        }
    }

    private  void fillModel() {
        List<Field> declaredFields = java.util.List.of(Record.class.getDeclaredFields());

        List<Record> records = recordService.readAll();
        for (Record record : records) {
            List<Object> row = new ArrayList<>();
            for (Field field : declaredFields) {
                try {
                    field.setAccessible(true);
                    row.add(field.get(record));
                } catch (Exception ex) {
                    ex.printStackTrace();
                    System.err.println(ex);
                }
            }
            model.addRow(row.toArray());
        }
    }

    private  void save(ActionEvent actionEvent) {
        Record record = Record.builder()
                .author(fields.get("author").getText())
                .tag(fields.get("tag").getText())
                .title(fields.get("title").getText())
//                .content(fields.get("content").getText())
                .build();

        recordService.create(record);
        clearModel();
        fillModel();
        clearForm();
    }

    private  void clearModel() {
        model.setRowCount(0);
    }
}
