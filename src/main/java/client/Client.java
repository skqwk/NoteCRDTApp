package client;

import client.mvc.dao.Dao;
import client.mvc.dao.RecordDao;
import client.mvc.model.Record;
import client.mvc.service.RecordService;
import client.sync.SyncService;
import client.mvc.view.ClientView;
import shared.crdt.hlc.HLC;
import shared.crdt.sqldriver.MessageDriver;

public class Client {
    public static void main(String[] args) {

        String nodeId = "client-" + (System.currentTimeMillis() % 1000);
        HLC clock = new HLC(System.currentTimeMillis(), nodeId);
        MessageDriver driver = new MessageDriver(clock);
        Dao<Record> recordDao = new RecordDao(driver);
        SyncService syncService = new SyncService(clock, driver);
        RecordService recordService = new RecordService(recordDao);

        new ClientView(nodeId, syncService, recordService).run();

    }
}
