import java.io.*;
import java.net.*;
import java.time.Instant;
import smartcity.Messages; // Pacote gerado pelo compilador protoc

public class sensor {
    // ====================================================================
    // CONFIGURAÇÕES DE REDE E IDENTIFICAÇÃO
    // ====================================================================
    private static final java.util.Random RNG = new java.util.Random();
    private static final String[][] SECTORS = {
        {"Pici", "pici"},
        {"Benfica", "benfica"},
        {"Porangabussu", "porangabussu"}
    };
    private static final int DEVICE_COUNT = Math.max(1, Integer.parseInt(System.getenv().getOrDefault("JAVA_DEVICE_COUNT", String.valueOf(SECTORS.length))));
    private static final java.util.Map<String, DeviceState> DEVICES = new java.util.LinkedHashMap<>();
    private static final java.util.List<String> DEVICE_ORDER = new java.util.ArrayList<>();
    private static final String DEFAULT_DEVICE_ID;
    private static final String GATEWAY_HOST = "gateway";
    private static final String DEVICE_HOSTNAME;
    static {
        String h = "sensor_semaforo";
        try { h = InetAddress.getLocalHost().getHostName(); } catch (Exception ignored) {}
        DEVICE_HOSTNAME = h;
    }

    private static final DatagramSocket DISC_SOCKET;
    static {
        try {
            DISC_SOCKET = new DatagramSocket();
        } catch (SocketException e) {
            throw new ExceptionInInitializerError(
                "Falha ao criar socket UDP de descoberta: " + e.getMessage()
            );
        }
    }

    // Portas segregadas para multiplexação espacial UDP
    private static final int GATEWAY_TELEMETRY_PORT = 5000;
    private static final int GATEWAY_DISCOVERY_PORT = 5002;
    
    private static final int CONTROL_TCP_PORT = 5003;
    private static final String MULTICAST_GROUP = "239.0.0.1";
    private static final int MULTICAST_PORT = 5000;
    private static final int UDP_MAX_RETRIES = 3;
    private static final long RETRY_BASE_DELAY_MS = 200L;
    private static final long RETRY_MAX_DELAY_MS = 1500L;
    private static final long TELEMETRY_JITTER_MS = 350L;
    private static final int MAX_TCP_FRAME_BYTES = 1024 * 1024;
    private static final long MANUAL_OVERRIDE_MS = 30_000L;
    private static final long THRESHOLD_SCAN_INTERVAL_MS = 1_000L;
    private static final long THRESHOLD_EVENT_COOLDOWN_MS = 3_000L;
    private static final int TRAFFIC_QUEUE_THRESHOLD = Integer.parseInt(
        System.getenv().getOrDefault("TRAFFIC_QUEUE_THRESHOLD", "35")
    );

    private static class DeviceState {
        final String deviceId;
        final String sector;
        volatile int currentStatus = Messages.DeviceStatus.STATUS_ON_VALUE;
        volatile int frequencySecs = 5;
        volatile long nextSendAtMillis = 0L;
        volatile long nextThresholdCheckAtMillis = 0L;
        volatile long lastThresholdSendAtMillis = 0L;
        volatile long manualUntilMillis = 0L;

        DeviceState(String deviceId, String sector) {
            this.deviceId = deviceId;
            this.sector = sector;
        }
    }

    static {
        for (int i = 0; i < DEVICE_COUNT; i++) {
            int sectorIdx = i % SECTORS.length;
            int sectorOrdinal = (i / SECTORS.length) + 1;
            String deviceId = "semaforo_" + SECTORS[sectorIdx][1] + "_"
                + String.format("%02d", sectorOrdinal);
            DeviceState device = new DeviceState(deviceId, SECTORS[sectorIdx][0]);
            DEVICES.put(deviceId, device);
            DEVICE_ORDER.add(deviceId);
        }
        DEFAULT_DEVICE_ID = DEVICE_ORDER.get(0);
    }

    public static void main(String[] args) {
        System.out.println("============================================================");
        System.out.println("[Java] Frota de semáforos iniciada (Arquitetura Multiplexada).");
        for (String deviceId : DEVICE_ORDER) {
            DeviceState device = DEVICES.get(deviceId);
            System.out.println("       Dispositivo=" + device.deviceId + " | Setor=" + device.sector);
        }
        System.out.println("============================================================");

        // 1. Injeção do Handshake inicial na rede de descoberta
        sendDiscovery();

        // 2. Alocação da Thread de Controle TCP (Atuação remota)
        new Thread(sensor::startTcpServer).start();

        // 3. Alocação da Thread de Recuperação via canal Multicast
        new Thread(sensor::startMulticastListener).start();

        // 4. Bloqueio da Thread Principal no Loop de Telemetria UDP
        runTelemetryLoop();
    }

    // ====================================================================
    // ROTINAS DE PROTOCOLO E COMUNICAÇÃO
    // ====================================================================

    private static long retryDelayMillis(int attempt) {
        long delay = RETRY_BASE_DELAY_MS;
        for (int i = 0; i < attempt; i++) {
            delay *= 2;
            if (delay >= RETRY_MAX_DELAY_MS) {
                delay = RETRY_MAX_DELAY_MS;
                break;
            }
        }
        return delay + RNG.nextInt((int) RETRY_BASE_DELAY_MS + 1);
    }

    private static long telemetryDelayMillis(int frequencySecs) {
        return (frequencySecs * 1000L) + RNG.nextInt((int) TELEMETRY_JITTER_MS + 1);
    }

    private static int randomDeviceStatus() {
        int roll = RNG.nextInt(100);
        if (roll < 78) return Messages.DeviceStatus.STATUS_ON_VALUE;
        if (roll < 90) return Messages.DeviceStatus.STATUS_OFF_VALUE;
        return Messages.DeviceStatus.STATUS_ERROR_VALUE;
    }

    private static int sampleQueueLength() {
        return 5 + RNG.nextInt(46);
    }

    private static String queueThresholdReason(int queueLength) {
        if (queueLength >= TRAFFIC_QUEUE_THRESHOLD) {
            return "queue_length=" + queueLength + " >= " + TRAFFIC_QUEUE_THRESHOLD;
        }
        return null;
    }

    private static boolean waitBeforeRetry(String channel, int attempt, Exception e) {
        long delay = retryDelayMillis(attempt);
        System.err.println("[Java:Retry] UDP " + channel + " falhou (tentativa "
            + (attempt + 1) + "/" + UDP_MAX_RETRIES + "): " + e.getMessage()
            + ". Retry em " + delay + "ms.");
        try {
            Thread.sleep(delay);
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static boolean sendUdpWithRetry(DatagramSocket socket, byte[] buf, int port, String channel) {
        for (int attempt = 0; attempt < UDP_MAX_RETRIES; attempt++) {
            try {
                InetAddress gateway = InetAddress.getByName(GATEWAY_HOST);
                socket.send(new DatagramPacket(buf, buf.length, gateway, port));
                return true;
            } catch (Exception e) {
                if (!waitBeforeRetry(channel, attempt, e)) {
                    System.err.println("[Java:Erro] UDP " + channel + " descartado após retries: " + e.getMessage());
                    return false;
                }
            }
        }
        System.err.println("[Java:Erro] UDP " + channel + " descartado após retries");
        return false;
    }

    /** Empacota e despacha o descritor de topologia para a porta 5002 */
    private static void sendDiscovery() {
        for (String deviceId : DEVICE_ORDER) {
            sendDiscovery(deviceId);
        }
    }

    private static void sendDiscovery(String deviceId) {
        DeviceState device = DEVICES.get(deviceId);
        if (device == null) return;

        // [M4] Reutiliza DISC_SOCKET estático — sem criação/destruição de socket por chamada
        try {
            Messages.DiscoveryResponse disc = Messages.DiscoveryResponse.newBuilder()
                .setDeviceId(device.deviceId)
                .setType(Messages.DeviceType.DEVICE_TYPE_TRAFFIC_LIGHT)
                .setIpAddress(DEVICE_HOSTNAME)
                .setControlPort(CONTROL_TCP_PORT)
                .setIsControllable(true)
                .setInitialStatus(Messages.DeviceStatus.forNumber(device.currentStatus))
                .build();

            byte[] buf = disc.toByteArray();

            // Roteamento exclusivo para o pipeline de Descoberta
            if (sendUdpWithRetry(DISC_SOCKET, buf, GATEWAY_DISCOVERY_PORT, "Descoberta")) {
                System.out.println("[Java:Descoberta] Dispositivo=" + device.deviceId
                    + " | Setor=" + device.sector
                    + " | Status=" + Messages.DeviceStatus.forNumber(device.currentStatus)
                    + " | Handshake de topologia emitido com sucesso.");
            }
        } catch (Exception e) {
            System.err.println("[Java:Erro] Falha ao despachar pacote de descoberta: " + e.getMessage());
        }
    }

    /** Instancia o servidor TCP implementando Length-Prefix Framing */
    private static void startTcpServer() {
        try (ServerSocket server = new ServerSocket(CONTROL_TCP_PORT)) {
            System.out.println("[Java:TCP] Interface de atuação aguardando conexões na porta " + CONTROL_TCP_PORT);
            
            while (true) {
                try (Socket s = server.accept();
                     DataInputStream in = new DataInputStream(s.getInputStream());
                     DataOutputStream out = new DataOutputStream(s.getOutputStream())) {
                    
                    // Framing: Extração estrita do prefixo de 4 bytes (Big-Endian nativo do Java)
                    int len = in.readInt();
                    if (len <= 0 || len > MAX_TCP_FRAME_BYTES) {
                        throw new IOException("Frame TCP inválido: " + len + " bytes");
                    }
                    byte[] payload = new byte[len];
                    in.readFully(payload);

                    // Desserialização segura a partir do tamanho extraído
                    Messages.ConfigCommand cmd = Messages.ConfigCommand.parseFrom(payload);
                    System.out.println("[Java:TCP] Comando interceptado. ID: " + cmd.getCommandId());

                    // Mutações de estado em memória volátil
                    String targetDeviceId = cmd.getTargetDeviceId().isBlank()
                        ? DEFAULT_DEVICE_ID
                        : cmd.getTargetDeviceId();
                    DeviceState target = DEVICES.get(targetDeviceId);
                    if (target == null) {
                        Messages.ConfigResponse resp = Messages.ConfigResponse.newBuilder()
                            .setCommandId(cmd.getCommandId())
                            .setSuccess(false)
                            .setMessage("Dispositivo alvo desconhecido no semáforo Java.")
                            .build();

                        byte[] respBuf = resp.toByteArray();
                        out.writeInt(respBuf.length);
                        out.write(respBuf);
                        continue;
                    }

                    if (cmd.getUpdateStatus()) {
                        target.currentStatus = cmd.getTargetStatus().getNumber();
                        target.manualUntilMillis = System.currentTimeMillis() + MANUAL_OVERRIDE_MS;
                        System.out.println("[Java:Atuação] Dispositivo=" + target.deviceId
                            + " | Setor=" + target.sector
                            + " | Transição de estado para: " + target.currentStatus);
                    }
                    if (cmd.getUpdateFrequency()) {
                        target.frequencySecs = cmd.getNewFrequencySecs();
                        System.out.println("[Java:Atuação] Dispositivo=" + target.deviceId
                            + " | Setor=" + target.sector
                            + " | Nova frequência de amostragem: " + target.frequencySecs + "s");
                    }
                    target.nextSendAtMillis = 0L;

                    // Construção do Frame de Confirmação (ACK)
                    Messages.ConfigResponse resp = Messages.ConfigResponse.newBuilder()
                        .setCommandId(cmd.getCommandId())
                        .setSuccess(true)
                        .setMessage("Semáforo Java " + target.deviceId + " reconfigurado com sucesso.")
                        .setUpdatedStatus(Messages.DeviceStatus.forNumber(target.currentStatus))
                        .setUpdatedFrequencySecs(target.frequencySecs)
                        .build();

                    byte[] respBuf = resp.toByteArray();
                    
                    // Aplicação do Framing na resposta: [Prefixo Int32] + [Vetor Binário]
                    out.writeInt(respBuf.length);
                    out.write(respBuf);
                    sendDiscovery(target.deviceId);
                } catch (Exception e) { 
                    System.err.println("[Java:Erro] Falha no pipeline TCP: " + e.getMessage()); 
                }
            }
        } catch (Exception e) { 
            e.printStackTrace(); 
        }
    }

    /** Intercepta Probes de Disaster Recovery do Hub Coordenador */
    private static void startMulticastListener() {
        try (MulticastSocket mc = new MulticastSocket(MULTICAST_PORT)) {
            InetAddress group = InetAddress.getByName(MULTICAST_GROUP);

            NetworkInterface ni = null;
            java.util.Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
            while (ifaces != null && ifaces.hasMoreElements()) {
                NetworkInterface iface = ifaces.nextElement();
                if (iface.isUp() && !iface.isLoopback() && iface.supportsMulticast()) {
                    ni = iface;
                    break;
                }
            }

            // Passa ni (pode ser null → JVM usa interface padrão do sistema)
            mc.joinGroup(new InetSocketAddress(group, MULTICAST_PORT), ni);
            System.out.println("[Java:Multicast] Inscrito no grupo de resiliência "
                + MULTICAST_GROUP + ":" + MULTICAST_PORT
                + (ni != null ? " via interface " + ni.getName() : " (interface padrão do sistema)"));
            
            byte[] buf = new byte[256];
            while (true) {
                DatagramPacket p = new DatagramPacket(buf, buf.length);
                mc.receive(p);
                
                String probeData = new String(p.getData(), 0, p.getLength());
                if (probeData.equals("SMARTCITY_DISCOVERY_PROBE")) {
                    System.out.println("[Java:Multicast] Probe de recuperação detectado. Re-sincronizando topologia!");
                    sendDiscovery();
                }
            }
        } catch (Exception e) { 
            e.printStackTrace(); 
        }
    }

    /** Motor contínuo de amostragem e despacho de DataPayload */
    private static void sendTelemetryPayload(
        DatagramSocket socket,
        DeviceState device,
        String triggerReason,
        Integer queueLengthOverride
    ) {
        long now = Instant.now().getEpochSecond();

        // Composição de ID único para idempotência no Gateway
        String msgId = device.deviceId + "-" + now + "-" + java.util.UUID.randomUUID().toString().substring(0, 5);

        Messages.DataPayload.Builder builder = Messages.DataPayload.newBuilder()
            .setMessageId(msgId)
            .setTimestamp(now)
            .setDeviceId(device.deviceId)
            .setCurrentStatus(Messages.DeviceStatus.forNumber(device.currentStatus));

        Integer queueLength = queueLengthOverride;
        if (device.currentStatus == Messages.DeviceStatus.STATUS_ON_VALUE) {
            if (queueLength == null) {
                queueLength = sampleQueueLength();
            }
            builder.addMetrics(Messages.Metric.newBuilder().setName("state").setValue(1).setUnit("code"));
            builder.addMetrics(Messages.Metric.newBuilder().setName("queue_length").setValue(queueLength).setUnit("vehicles"));
        }

        Messages.DataPayload p = builder.build();
        byte[] b = p.toByteArray();

        // Roteamento estrito para o pipeline de Telemetria contínua
        if (sendUdpWithRetry(socket, b, GATEWAY_TELEMETRY_PORT, "Telemetria")) {
            String eventLabel = triggerReason == null ? "Telemetria injetada" : "Evento por limiar";
            System.out.println("[Java:UDP] " + eventLabel
                + " | Dispositivo=" + device.deviceId
                + " | Setor=" + device.sector
                + " | ID=" + msgId
                + " | Status=" + Messages.DeviceStatus.forNumber(device.currentStatus)
                + (queueLength == null ? "" : " | Fila=" + queueLength + " veiculos")
                + (triggerReason == null ? "" : " | Limiar=" + triggerReason));
        }
    }

    private static void pollThresholdEvent(DatagramSocket socket, DeviceState device, long nowMillis) {
        if (nowMillis < device.nextThresholdCheckAtMillis) {
            return;
        }
        device.nextThresholdCheckAtMillis = nowMillis + THRESHOLD_SCAN_INTERVAL_MS;

        if (
            device.currentStatus != Messages.DeviceStatus.STATUS_ON_VALUE
            || nowMillis - device.lastThresholdSendAtMillis < THRESHOLD_EVENT_COOLDOWN_MS
        ) {
            return;
        }

        int queueLength = sampleQueueLength();
        String triggerReason = queueThresholdReason(queueLength);
        if (triggerReason != null) {
            device.lastThresholdSendAtMillis = nowMillis;
            sendTelemetryPayload(socket, device, triggerReason, queueLength);
        }
    }

    private static void runTelemetryLoop() {
        try (DatagramSocket socket = new DatagramSocket()) {
            while (true) {
                long nowMillis = System.currentTimeMillis();
                for (String deviceId : DEVICE_ORDER) {
                    DeviceState device = DEVICES.get(deviceId);
                    if (device == null) {
                        continue;
                    }

                    pollThresholdEvent(socket, device, nowMillis);

                    if (nowMillis < device.nextSendAtMillis) {
                        continue;
                    }

                    if (nowMillis >= device.manualUntilMillis) {
                        device.currentStatus = randomDeviceStatus();
                    }
                    device.nextSendAtMillis = nowMillis + telemetryDelayMillis(device.frequencySecs);

                    Integer queueLength = null;
                    if (device.currentStatus == Messages.DeviceStatus.STATUS_ON_VALUE) {
                        queueLength = sampleQueueLength();
                        if (queueThresholdReason(queueLength) != null) {
                            device.lastThresholdSendAtMillis = nowMillis;
                        }
                    }

                    sendTelemetryPayload(socket, device, null, queueLength);
                }
                
                // Suspensão curta: cada dispositivo gerencia sua própria janela de amostragem.
                Thread.sleep(200L);
            }
        } catch (Exception e) { 
            e.printStackTrace(); 
        }
    }
}
