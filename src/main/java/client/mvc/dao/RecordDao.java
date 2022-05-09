package client.mvc.dao;


import client.mvc.model.Record;
import shared.crdt.sqldriver.MessageDriver;

import java.util.List;
import java.util.Map;

public class RecordDao implements Dao<Record> {

    private final MessageDriver messageDriver;

    public RecordDao(MessageDriver messageDriver) {
        this.messageDriver = messageDriver;
    }


    @Override
    public void create(Record entity) {
        entity.setId(System.currentTimeMillis());
        messageDriver.write(entity, Record.class);
    }

    @Override
    public Record read(Long id) {
        return messageDriver.read(id, Record.class);
    }

    @Override
    public void update(Long id, Map<String, Object> updatedFields) {
        messageDriver.replace(id, updatedFields, Record.class);
    }

    @Override
    public void delete(Long id) {
        messageDriver.markAsDeleted(id, Record.class);
    }

    @Override
    public List<Record> readAll() {
        return messageDriver.readAll(Record.class);
    }
}
