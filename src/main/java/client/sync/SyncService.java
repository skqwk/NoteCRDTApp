package client.sync;

import shared.crdt.hlc.HLC;
import shared.crdt.hlc.HybridTimestamp;
import shared.crdt.sqldriver.MessageDriver;
import shared.crdt.sqldriver.StorageDriver;
import shared.Payload;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Map;

public class SyncService {


    private final HLC clock;
    private HybridTimestamp lastSend;
    private HybridTimestamp lastReceive;

    private final MessageDriver messageDriver;
    private final StorageDriver storageDriver;

    public SyncService(HLC clock, MessageDriver messageDriver) {
        this.clock = clock;
        this.messageDriver = messageDriver;
        this.storageDriver = new StorageDriver(clock.getNodeId());

        Object storageLastSend = storageDriver.get("last_send");
        this.lastSend = storageLastSend == null
                ? null :
                HybridTimestamp.parse(storageLastSend.toString());

        Object storageLastReceive = storageDriver.get("last_receive");
        this.lastReceive = storageLastReceive == null
                ? null :
                HybridTimestamp.parse(storageLastReceive.toString());
    }

    public void send() {
        try(Socket socket = new Socket("127.0.0.1", 8080);
            ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream input = new ObjectInputStream(socket.getInputStream())
            ) {

            // Подготовка к отправке на сервер
            List<Map<String, Object>> messages = lastSend == null
                    ? messageDriver.readAllMessages()
                    : messageDriver.readAllSince(lastSend);
            if (lastSend == null) {
                lastSend = clock.now();
            } else {
                lastSend = clock.tick(lastSend);
            }
            storageDriver.put("last_send", lastSend);

            // Отправка на сервер
            Payload request = Payload.builder()
                    .messages(messages)
                    .timestamp(lastReceive == null ? null : lastReceive.toString())
                    .build();
            output.writeObject(request);

            // Получение ответа от сервера
            Payload response =  (Payload) input.readObject();
            lastReceive = HybridTimestamp.parse(response.timestamp);
            List<Map<String, Object>> remoteMessages = response.messages;

            storageDriver.put("last_receive", lastReceive);
            messageDriver.merge(remoteMessages);

            System.out.println("Server response: " + response);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
