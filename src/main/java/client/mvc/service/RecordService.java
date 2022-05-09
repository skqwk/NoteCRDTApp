package client.mvc.service;

import client.mvc.dao.Dao;
import client.mvc.model.Record;

public class RecordService extends Service<Record> {

    public RecordService(Dao<Record> recordDao) {
        super(recordDao);
    }

}
