package server;

import shared.crdt.hlc.HLC;
import shared.crdt.hlc.HybridTimestamp;
import shared.crdt.sqldriver.MessageDriver;
import shared.Payload;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;

public class Server {

    private final MessageDriver messageDriver;
    private final HLC clock;

    public Server() {
        String nodeId = "server";
        this.clock = new HLC(System.currentTimeMillis(), nodeId);
        this.messageDriver = new MessageDriver(clock);
    }

    public static void main(String[] args)  {
        new Server().work();
    }

    public void work() {
        while (true) {
            try (ServerSocket serverSocket = new ServerSocket(8080);
                 Socket socket = serverSocket.accept();
                 ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
                 ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream())
            ) {

                // Запрос от клиента
                Payload request = (Payload) input.readObject();
                System.out.println("Client request: " + request);


                // Запись сообщений от клиента в базу
                messageDriver.writeMessages(request.messages);

                HybridTimestamp lastReceive;
                List<Map<String, Object>> messages;
//                if (request.timestamp != null) {
//                    lastReceive = HybridTimestamp.parse(request.timestamp);
//                    messages = messageDriver.readAllSince(lastReceive);
//                } else {
//                    messages = messageDriver.readAllMessages();
//                }
                messages = messageDriver.readAllMessages();

                // Подготовка к ответу
                System.out.println("Rows size = " + messages.size());
                lastReceive = HybridTimestamp.parse(messages.get(0).get("timestamp").toString());
                for (Map<String, Object> message : messages) {
                    lastReceive = lastReceive.max(HybridTimestamp.parse(message.get("timestamp").toString()));
                }


                Payload response = Payload.builder()
                        .messages(messages)
                        .timestamp(lastReceive.toString())
                        .build();
                // Отправка ответа клиенту
                output.writeObject(response);

            } catch ( EOFException ex) {
                System.out.println("Client close socket");
            } catch (IOException | ClassNotFoundException ex) {
                ex.printStackTrace();
            }
        }
    }


}
