package client.hlc;


import java.util.Arrays;

public class HLC {

    private HybridTimestamp latestTime;
    private final String nodeId;

    // (1) Инициализация
    public HLC(long currentTimeMillis, String nodeId) {
        this.latestTime = new HybridTimestamp(currentTimeMillis, 0, nodeId);
        this.nodeId = nodeId;
    }

    // (2) Отправка или локальное событие на узле
    // Каждый раз, когда произошло локальное событие, гибридная метка времени сопоставляется с изменением.
    // Нужно сравнить системное время узла и события, если узел отстает, тогда увеличить другое значение,
    // которое представляет логическую часть компонента (ticks), чтобы отразить ход часов.
    public HybridTimestamp now() {

        long currentTimeMillis = System.currentTimeMillis();

        if (latestTime.getWallClockTime() >= currentTimeMillis) {
            latestTime = latestTime.addTicks(1);
        } else {
            latestTime = new HybridTimestamp(currentTimeMillis, 0, nodeId);
        }
        return latestTime;
    }

    // (3) Получение сообщения из удаленного узла
    public HybridTimestamp tick(HybridTimestamp remoteTimestamp) {
        // Данный метод возвращает временную метку с системным временем и логическим компонентом
        // Установленным в -1, но он вернется в 0, после addTicks(1);
        long nowMillis = System.currentTimeMillis();
        HybridTimestamp now = HybridTimestamp.fromSystemTime(nowMillis, nodeId);

        // Выбираем максимальную временную метку из 3ех:
        // [1] - Текущее системное время
        // [2] - Временная метка другого клиента
        // [3] - Временная метка данного клиента
        latestTime = max(now, remoteTimestamp, latestTime);
        latestTime = latestTime.addTicks(1);
        return latestTime;
    }

    private HybridTimestamp max(HybridTimestamp... times) {
        return Arrays.stream(times).max(HybridTimestamp::compareTo).get();
    }

    public HybridTimestamp getLatestTime() {
        return latestTime;
    }

    public String getNodeId() {
        return this.nodeId;
    }
}
