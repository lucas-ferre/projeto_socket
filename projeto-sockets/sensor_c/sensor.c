#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <pthread.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include "messages.pb-c.h"

#define GATEWAY_HOST          "gateway"
#define GATEWAY_TELEMETRY_PORT "5000"
#define GATEWAY_DISCOVERY_PORT "5002"
#define MULTICAST_GROUP       "239.0.0.1"
#define MULTICAST_PORT        5000
#define SLEEP_INTERVAL_SECS   5
#define UDP_MAX_RETRIES       3
#define UDP_RETRY_BASE_USEC   200000
#define UDP_RETRY_MAX_USEC    1500000
#define TELEMETRY_JITTER_USEC 500000
#define DEVICE_COUNT          3

// Número total de métricas emitidas por ciclo de telemetria
#define NUM_METRICS 6

// ====================================================================
// VARIÁVEIS GLOBAIS E DE ESTADO DO CICLO DE VIDA
// ====================================================================

volatile sig_atomic_t keep_running = 1;

int global_sockfd = -1;
struct addrinfo *global_gateway_telemetry_res = NULL;
struct addrinfo *global_gateway_discovery_res = NULL;
pthread_t listener_tid;

char global_device_ids[DEVICE_COUNT][64];
const char *global_device_sectors[DEVICE_COUNT];
Smartcity__DeviceStatus global_device_statuses[DEVICE_COUNT];

static const char *SENSOR_SECTORS[] = { "Pici", "Benfica", "Porangabussu" };
static const char *SENSOR_SECTOR_SLUGS[] = { "pici", "benfica", "porangabussu" };
static const int SENSOR_SECTOR_COUNT = 3;

// ====================================================================
// TRATAMENTO DE SINAIS
// ====================================================================

void handle_shutdown_signal(int sig) {
    keep_running = 0;
}

void multicast_socket_cleanup(void *arg) {
    int *sock = (int *)arg;
    if (sock && *sock >= 0) {
        close(*sock);
        printf("[Sensor C:Teardown] Socket Multicast da thread desvinculado e encerrado.\n");
    }
}

// ====================================================================
// CÁLCULO DE QUALIDADE DO AR
// ====================================================================

/**
 * Calcula o AQI (Air Quality Index) a partir da concentração de PM2.5.
 * Fórmula linear por faixas segundo o padrão EPA dos EUA.
 *
 * Faixas de PM2.5 (µg/m³) → faixas de AQI:
 *   0.0  – 12.0  →   0 –  50  (Bom)
 *  12.1  – 35.4  →  51 – 100  (Moderado)
 *  35.5  – 55.4  → 101 – 150  (Insalubre para grupos sensíveis)
 *  55.5  – 150.4 → 151 – 200  (Insalubre)
 * 150.5  – 250.4 → 201 – 300  (Muito insalubre)
 * 250.5  – 350.4 → 301 – 400  (Perigoso)
 * 350.5  – 500.4 → 401 – 500  (Perigoso extremo)
 */
static double compute_aqi(double pm25) {
    static const double c_lo[] = {  0.0,  12.1,  35.5,  55.5, 150.5, 250.5, 350.5 };
    static const double c_hi[] = { 12.0,  35.4,  55.4, 150.4, 250.4, 350.4, 500.4 };
    static const int    i_lo[] = {    0,    51,   101,   151,   201,   301,   401 };
    static const int    i_hi[] = {   50,   100,   150,   200,   300,   400,   500 };

    for (int k = 0; k < 7; k++) {
        if (pm25 >= c_lo[k] && pm25 <= c_hi[k]) {
            return ((double)(i_hi[k] - i_lo[k]) / (c_hi[k] - c_lo[k]))
                   * (pm25 - c_lo[k]) + i_lo[k];
        }
    }
    return 500.0; // Perigoso extremo
}

static Smartcity__DeviceStatus random_device_status() {
    int roll = rand() % 100;
    if (roll < 78) return SMARTCITY__DEVICE_STATUS__STATUS_ON;
    if (roll < 90) return SMARTCITY__DEVICE_STATUS__STATUS_OFF;
    return SMARTCITY__DEVICE_STATUS__STATUS_ERROR;
}

static const char *status_to_text(Smartcity__DeviceStatus status) {
    switch (status) {
        case SMARTCITY__DEVICE_STATUS__STATUS_ON: return "STATUS_ON";
        case SMARTCITY__DEVICE_STATUS__STATUS_OFF: return "STATUS_OFF";
        case SMARTCITY__DEVICE_STATUS__STATUS_ERROR: return "STATUS_ERROR";
        default: return "STATUS_UNKNOWN";
    }
}

static useconds_t retry_delay_usec(int attempt) {
    long delay = UDP_RETRY_BASE_USEC;
    for (int i = 0; i < attempt; i++) {
        delay *= 2;
        if (delay >= UDP_RETRY_MAX_USEC) {
            delay = UDP_RETRY_MAX_USEC;
            break;
        }
    }
    delay += rand() % UDP_RETRY_BASE_USEC;
    return (useconds_t)delay;
}

static void sleep_with_jitter(unsigned int base_secs) {
    sleep(base_secs);
    if (keep_running) {
        usleep((useconds_t)(rand() % (TELEMETRY_JITTER_USEC + 1)));
    }
}

static int resolve_gateway_with_retry(const char *port,
                                      const struct addrinfo *hints,
                                      struct addrinfo **result,
                                      const char *channel) {
    int rc = EAI_FAIL;
    for (int attempt = 0; attempt < UDP_MAX_RETRIES; attempt++) {
        rc = getaddrinfo(GATEWAY_HOST, port, hints, result);
        if (rc == 0) return 0;

        fprintf(stderr,
                "[Sensor C:Retry] DNS %s falhou (tentativa %d/%d): %s\n",
                channel, attempt + 1, UDP_MAX_RETRIES, gai_strerror(rc));
        if (attempt < UDP_MAX_RETRIES - 1) {
            usleep(retry_delay_usec(attempt));
        }
    }
    return rc;
}

static int send_udp_with_retry(const uint8_t *buffer,
                               size_t len,
                               struct addrinfo *target,
                               const char *channel) {
    if (target == NULL || global_sockfd < 0) return -1;

    for (int attempt = 0; attempt < UDP_MAX_RETRIES; attempt++) {
        ssize_t sent = sendto(global_sockfd, buffer, len, 0,
                              target->ai_addr, target->ai_addrlen);
        if (sent == (ssize_t)len) return 0;

        int err = errno;
        fprintf(stderr,
                "[Sensor C:Retry] UDP %s falhou (tentativa %d/%d): %s\n",
                channel, attempt + 1, UDP_MAX_RETRIES, strerror(err));
        if (attempt < UDP_MAX_RETRIES - 1) {
            usleep(retry_delay_usec(attempt));
        }
    }

    return -1;
}

// ====================================================================
// ROTINAS DE PROTOCOLO E SERIALIZAÇÃO BINÁRIA
// ====================================================================

void send_discovery_announcement() {
    if (global_gateway_discovery_res == NULL || global_sockfd < 0) return;

    for (int i = 0; i < DEVICE_COUNT; i++) {
        Smartcity__DiscoveryResponse disc = SMARTCITY__DISCOVERY_RESPONSE__INIT;
        disc.device_id      = global_device_ids[i];
        disc.type           = SMARTCITY__DEVICE_TYPE__DEVICE_TYPE_WEATHER_STATION;
        disc.ip_address     = "sensor_temperatura";
        disc.initial_status = global_device_statuses[i];
        disc.is_controllable = 0;
        disc.control_port   = 0;

        size_t packed_size = smartcity__discovery_response__get_packed_size(&disc);
        uint8_t *buffer = malloc(packed_size);
        if (buffer) {
            smartcity__discovery_response__pack(&disc, buffer);
            if (send_udp_with_retry(buffer, packed_size, global_gateway_discovery_res, "Descoberta") == 0) {
                printf("[Sensor C:Descoberta] Dispositivo=%s | Setor=%s | Status=%s | Handshake de presença injetado via porta %s.\n",
                       global_device_ids[i], global_device_sectors[i],
                       status_to_text(global_device_statuses[i]), GATEWAY_DISCOVERY_PORT);
            } else {
                fprintf(stderr, "[Sensor C:Erro] Handshake de descoberta de %s descartado após retries.\n",
                        global_device_ids[i]);
            }
            free(buffer);
        }
    }
}

void *multicast_listener_thread(void *arg) {
    int mc_sock = -1;
    struct sockaddr_in mc_addr;
    char buffer[256];

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    if ((mc_sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        perror("[Sensor C:Thread] Falha na alocação de descritor Multicast");
        pthread_exit(NULL);
    }

    pthread_cleanup_push(multicast_socket_cleanup, &mc_sock);

    int reuse = 1;
    setsockopt(mc_sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    memset(&mc_addr, 0, sizeof(mc_addr));
    mc_addr.sin_family      = AF_INET;
    mc_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    mc_addr.sin_port        = htons(MULTICAST_PORT);

    if (bind(mc_sock, (struct sockaddr *)&mc_addr, sizeof(mc_addr)) < 0) {
        perror("[Sensor C:Thread] Violação de bind na interface Multicast");
        pthread_exit(NULL);
    }

    struct ip_mreq mreq;
    mreq.imr_multiaddr.s_addr = inet_addr(MULTICAST_GROUP);
    mreq.imr_interface.s_addr = htonl(INADDR_ANY);
    setsockopt(mc_sock, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));

    printf("[Sensor C:Thread] Escutando datagramas Multicast em %s:%d\n",
           MULTICAST_GROUP, MULTICAST_PORT);

    struct sockaddr_in sender_addr;
    socklen_t sender_len = sizeof(sender_addr);

    while (1) {
        ssize_t n = recvfrom(mc_sock, buffer, sizeof(buffer) - 1, 0,
                             (struct sockaddr *)&sender_addr, &sender_len);
        if (n > 0) {
            buffer[n] = '\0';
            if (strcmp(buffer, "SMARTCITY_DISCOVERY_PROBE") == 0) {
                printf("[Sensor C:Thread] Probe interceptado — re-sincronizando topologia.\n");
                send_discovery_announcement();
            }
        }
    }

    pthread_cleanup_pop(1);
    return NULL;
}

// ====================================================================
// MOTOR DE EXECUÇÃO PRINCIPAL
// ====================================================================

int main() {
    srand(time(NULL) ^ getpid()); // Entropia híbrida (tempo + ID do processo)

    printf("============================================================\n");
    printf("[Sensor C] Inicializando frota de %d Estações Ambientais...\n", DEVICE_COUNT);
    for (int i = 0; i < DEVICE_COUNT; i++) {
        int sector_idx = i % SENSOR_SECTOR_COUNT;
        global_device_sectors[i] = SENSOR_SECTORS[sector_idx];
        global_device_statuses[i] = SMARTCITY__DEVICE_STATUS__STATUS_ON;
        snprintf(global_device_ids[i], sizeof(global_device_ids[i]),
                 "estacao_%s_%04X", SENSOR_SECTOR_SLUGS[sector_idx], rand() % 0xFFFF);
        printf("           Dispositivo=%s | Setor=%s\n",
               global_device_ids[i], global_device_sectors[i]);
    }
    printf("           Métricas: temperatura, umidade, CO₂, PM2.5, PM10, AQI\n");
    printf("============================================================\n");

    // 1. Mapeamento de sinais POSIX para Graceful Shutdown
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_shutdown_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);

    // 2. Resolução DNS bifurcada
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;

    if (resolve_gateway_with_retry(GATEWAY_TELEMETRY_PORT,
                                   &hints, &global_gateway_telemetry_res,
                                   "Telemetria") != 0) {
        fprintf(stderr, "[Sensor C:Erro] Falha ao resolver DNS para Telemetria.\n");
        exit(EXIT_FAILURE);
    }
    if (resolve_gateway_with_retry(GATEWAY_DISCOVERY_PORT,
                                   &hints, &global_gateway_discovery_res,
                                   "Descoberta") != 0) {
        fprintf(stderr, "[Sensor C:Erro] Falha ao resolver DNS para Descoberta.\n");
        freeaddrinfo(global_gateway_telemetry_res);
        exit(EXIT_FAILURE);
    }

    global_sockfd = socket(global_gateway_telemetry_res->ai_family,
                           global_gateway_telemetry_res->ai_socktype,
                           global_gateway_telemetry_res->ai_protocol);
    if (global_sockfd < 0) {
        perror("[Sensor C:Erro] Falha na criação do socket");
        freeaddrinfo(global_gateway_telemetry_res);
        freeaddrinfo(global_gateway_discovery_res);
        exit(EXIT_FAILURE);
    }

    // 3. Handshake topológico inicial
    send_discovery_announcement();

    // 4. Thread de escuta Multicast
    if (pthread_create(&listener_tid, NULL, multicast_listener_thread, NULL) != 0)
        perror("[Sensor C:Aviso] Falha ao criar thread Multicast");

    // ----------------------------------------------------------------
    // 5. Inicialização estática das 6 métricas ambientais
    // ----------------------------------------------------------------
    Smartcity__DataPayload payload = SMARTCITY__DATA_PAYLOAD__INIT;

    Smartcity__Metric metrics[NUM_METRICS];
    Smartcity__Metric *metrics_list[NUM_METRICS];

    // Nomes e unidades fixos — valores são atualizados a cada ciclo
    char *names[] = { "temperature", "humidity", "co2", "pm25", "pm10", "aqi" };
    char *units[] = {          "C",       "%",   "ppm", "ug/m3", "ug/m3", "index" };

    for (int i = 0; i < NUM_METRICS; i++) {
        metrics[i]      = (Smartcity__Metric)SMARTCITY__METRIC__INIT;
        metrics[i].name = names[i];
        metrics[i].unit = units[i];
        metrics_list[i] = &metrics[i];
    }

    unsigned int seq_counter = 0;
    char msg_id_buffer[80];

    // 6. Loop principal de telemetria
    while (keep_running) {
        for (int device_idx = 0; device_idx < DEVICE_COUNT; device_idx++) {
            time_t now = time(NULL);
            global_device_statuses[device_idx] = random_device_status();

            snprintf(msg_id_buffer, sizeof(msg_id_buffer),
                     "%s-%ld-%u", global_device_ids[device_idx], now, seq_counter++);

            payload.message_id = msg_id_buffer;
            payload.timestamp  = now;
            payload.device_id = global_device_ids[device_idx];
            payload.current_status = global_device_statuses[device_idx];

            double temperature = 25.0 + ((double)rand() / RAND_MAX) * 10.0;
            double humidity = 55.0 + ((double)rand() / RAND_MAX) * 35.0;
            double co2 = 400.0 + ((double)rand() / RAND_MAX) * 200.0;
            double pm25 = 5.0 + ((double)rand() / RAND_MAX) * 40.0;
            double pm10 = pm25 + 5.0 + ((double)rand() / RAND_MAX) * 20.0;
            double aqi = compute_aqi(pm25);

            metrics[0].value = temperature;
            metrics[1].value = humidity;
            metrics[2].value = co2;
            metrics[3].value = pm25;
            metrics[4].value = pm10;
            metrics[5].value = aqi;

            if (payload.current_status == SMARTCITY__DEVICE_STATUS__STATUS_ON) {
                payload.n_metrics = NUM_METRICS;
                payload.metrics = metrics_list;
            } else {
                payload.n_metrics = 0;
                payload.metrics = NULL;
            }

            size_t len = smartcity__data_payload__get_packed_size(&payload);
            uint8_t *buf = malloc(len);
            int sent_ok = 0;
            if (buf) {
                smartcity__data_payload__pack(&payload, buf);
                sent_ok = (send_udp_with_retry(buf, len, global_gateway_telemetry_res, "Telemetria") == 0);
                free(buf);
            } else {
                fprintf(stderr, "[Sensor C:Erro] Falha de alocação para serializar telemetria.\n");
            }

            if (sent_ok && payload.current_status == SMARTCITY__DEVICE_STATUS__STATUS_ON) {
                printf("[Sensor C:UDP] Dispositivo=%s | Setor=%s | Status=%s | ID=%s | "
                       "Temp=%.1f°C  UR=%.0f%%  "
                       "CO2=%.0fppm  PM2.5=%.1f  PM10=%.1f  AQI=%.0f\n",
                       global_device_ids[device_idx], global_device_sectors[device_idx],
                       status_to_text(payload.current_status), msg_id_buffer,
                       temperature, humidity,
                       co2, pm25, pm10, aqi);
            } else if (sent_ok) {
                printf("[Sensor C:UDP] Heartbeat operacional | Dispositivo=%s | Setor=%s | Status=%s | ID=%s\n",
                       global_device_ids[device_idx], global_device_sectors[device_idx],
                       status_to_text(payload.current_status), msg_id_buffer);
            } else {
                fprintf(stderr, "[Sensor C:UDP] Telemetria ID=%s descartada após retries.\n",
                        msg_id_buffer);
            }
        }

        sleep_with_jitter(SLEEP_INTERVAL_SECS);
    }

    // ====================================================================
    // TEARDOWN
    // ====================================================================
    printf("\n[Sensor C] Sinal POSIX interceptado. Iniciando Teardown...\n");

    pthread_cancel(listener_tid);
    pthread_join(listener_tid, NULL);

    if (global_gateway_telemetry_res) {
        freeaddrinfo(global_gateway_telemetry_res);
        global_gateway_telemetry_res = NULL;
        printf("[Sensor C:Teardown] DNS Telemetria liberado.\n");
    }
    if (global_gateway_discovery_res) {
        freeaddrinfo(global_gateway_discovery_res);
        global_gateway_discovery_res = NULL;
        printf("[Sensor C:Teardown] DNS Descoberta liberado.\n");
    }
    if (global_sockfd >= 0) {
        close(global_sockfd);
        global_sockfd = -1;
        printf("[Sensor C:Teardown] Socket principal encerrado.\n");
    }

    printf("[Sensor C] Encerrado com código POSIX 0.\n");
    return 0;
}
