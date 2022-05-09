package client.service;

import client.dao.Dao;
import client.model.Record;

public class RecordService extends Service<Record> {

    public RecordService(Dao<Record> recordDao) {
        super(recordDao);
    }

}
