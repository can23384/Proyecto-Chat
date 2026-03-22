#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "protocolo.h"

#define MAX_CLIENTS 100
#define BACKLOG 20

typedef struct {
    char username[32];
    char ip[INET_ADDRSTRLEN];
    char status[16];
    int sockfd;
    int activo;
    time_t ultimo_mensaje;
} Cliente;

static Cliente lista[MAX_CLIENTS];
static pthread_mutex_t mutex_lista = PTHREAD_MUTEX_INITIALIZER;
static volatile sig_atomic_t server_running = 1;
static int g_serverfd = -1;

static void log_msg(const char *tag, const char *fmt, ...) {
    time_t now = time(NULL);
    struct tm tm_info;
    localtime_r(&now, &tm_info);

    char ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_info);

    printf("[%s] [%s] ", ts, tag);

    va_list args;
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);

    printf("\n");
    fflush(stdout);
}

static const char *command_to_string(uint8_t cmd) {
    switch (cmd) {
        case CMD_REGISTER:     return "CMD_REGISTER";
        case CMD_BROADCAST:    return "CMD_BROADCAST";
        case CMD_DIRECT:       return "CMD_DIRECT";
        case CMD_LIST:         return "CMD_LIST";
        case CMD_INFO:         return "CMD_INFO";
        case CMD_STATUS:       return "CMD_STATUS";
        case CMD_LOGOUT:       return "CMD_LOGOUT";
        case CMD_OK:           return "CMD_OK";
        case CMD_ERROR:        return "CMD_ERROR";
        case CMD_MSG:          return "CMD_MSG";
        case CMD_USER_LIST:    return "CMD_USER_LIST";
        case CMD_USER_INFO:    return "CMD_USER_INFO";
        case CMD_DISCONNECTED: return "CMD_DISCONNECTED";
        default:               return "CMD_UNKNOWN";
    }
}

static void handle_sigint(int sig) {
    (void)sig;
    server_running = 0;

    if (g_serverfd != -1) {
        close(g_serverfd);
        g_serverfd = -1;
    }
}

static ssize_t send_all(int fd, const void *buf, size_t len) {
    size_t total = 0;
    const char *p = (const char *)buf;

    while (total < len) {
        ssize_t sent = send(fd, p + total, len - total, 0);
        if (sent < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (sent == 0) {
            return -1;
        }
        total += (size_t)sent;
    }

    return (ssize_t)total;
}

static ssize_t recv_all(int fd, void *buf, size_t len) {
    size_t total = 0;
    char *p = (char *)buf;

    while (total < len) {
        ssize_t recvd = recv(fd, p + total, len - total, 0);
        if (recvd < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (recvd == 0) {
            return 0;
        }
        total += (size_t)recvd;
    }

    return (ssize_t)total;
}

static void packet_zero(ChatPacket *pkt) {
    memset(pkt, 0, sizeof(*pkt));
}

static size_t bounded_strlen(const char *s, size_t max_len) {
    size_t i = 0;
    while (i < max_len && s[i] != '\0') {
        i++;
    }
    return i;
}

static void safe_copy(char *dst, size_t dst_size, const char *src) {
    if (dst_size == 0) {
        return;
    }
    if (src == NULL) {
        dst[0] = '\0';
        return;
    }
    strncpy(dst, src, dst_size - 1);
    dst[dst_size - 1] = '\0';
}

static int send_packet(int fd, uint8_t command, const char *sender,
                       const char *target, const char *payload) {
    ChatPacket pkt;
    packet_zero(&pkt);

    pkt.command = command;
    safe_copy(pkt.sender, sizeof(pkt.sender), sender);
    safe_copy(pkt.target, sizeof(pkt.target), target);
    safe_copy(pkt.payload, sizeof(pkt.payload), payload);
    pkt.payload_len = (uint16_t)bounded_strlen(pkt.payload, sizeof(pkt.payload));

    return (send_all(fd, &pkt, sizeof(pkt)) == (ssize_t)sizeof(pkt)) ? 0 : -1;
}

static int recv_packet(int fd, ChatPacket *pkt) {
    packet_zero(pkt);

    ssize_t n = recv_all(fd, pkt, sizeof(*pkt));
    if (n <= 0) {
        return (int)n;
    }

    pkt->sender[sizeof(pkt->sender) - 1] = '\0';
    pkt->target[sizeof(pkt->target) - 1] = '\0';
    pkt->payload[sizeof(pkt->payload) - 1] = '\0';

    if (pkt->payload_len >= sizeof(pkt->payload)) {
        pkt->payload_len = sizeof(pkt->payload) - 1;
    }
    pkt->payload[pkt->payload_len] = '\0';

    return 1;
}

static int is_valid_status(const char *status) {
    return strcmp(status, STATUS_ACTIVO) == 0 ||
           strcmp(status, STATUS_OCUPADO) == 0 ||
           strcmp(status, STATUS_INACTIVO) == 0;
}

static int find_client_index_by_username(const char *username) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (lista[i].activo && strcmp(lista[i].username, username) == 0) {
            return i;
        }
    }
    return -1;
}

static int find_client_index_by_ip(const char *ip) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (lista[i].activo && strcmp(lista[i].ip, ip) == 0) {
            return i;
        }
    }
    return -1;
}

static int find_client_index_by_sockfd(int sockfd) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (lista[i].activo && lista[i].sockfd == sockfd) {
            return i;
        }
    }
    return -1;
}

static int add_client(const char *username, const char *ip, int sockfd) {
    int result = -1;

    pthread_mutex_lock(&mutex_lista);

    if (find_client_index_by_username(username) != -1) {
        result = -2;
        goto done;
    }

    if (find_client_index_by_ip(ip) != -1) {
        result = -3;
        goto done;
    }

    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (!lista[i].activo) {
            memset(&lista[i], 0, sizeof(lista[i]));
            safe_copy(lista[i].username, sizeof(lista[i].username), username);
            safe_copy(lista[i].ip, sizeof(lista[i].ip), ip);
            safe_copy(lista[i].status, sizeof(lista[i].status), STATUS_ACTIVO);
            lista[i].sockfd = sockfd;
            lista[i].activo = 1;
            lista[i].ultimo_mensaje = time(NULL);
            result = i;
            break;
        }
    }

done:
    pthread_mutex_unlock(&mutex_lista);
    return result;
}

static int remove_client_by_sockfd(int sockfd, char *username_out, size_t username_out_size) {
    int removed = 0;

    pthread_mutex_lock(&mutex_lista);
    int idx = find_client_index_by_sockfd(sockfd);
    if (idx != -1) {
        if (username_out != NULL && username_out_size > 0) {
            safe_copy(username_out, username_out_size, lista[idx].username);
        }
        memset(&lista[idx], 0, sizeof(lista[idx]));
        removed = 1;
    }
    pthread_mutex_unlock(&mutex_lista);

    return removed;
}

static void touch_client_activity(int sockfd) {
    pthread_mutex_lock(&mutex_lista);
    int idx = find_client_index_by_sockfd(sockfd);
    if (idx != -1) {
        lista[idx].ultimo_mensaje = time(NULL);
    }
    pthread_mutex_unlock(&mutex_lista);
}

static int set_client_status_by_sockfd(int sockfd, const char *status) {
    int ok = 0;

    pthread_mutex_lock(&mutex_lista);
    int idx = find_client_index_by_sockfd(sockfd);
    if (idx != -1) {
        safe_copy(lista[idx].status, sizeof(lista[idx].status), status);
        lista[idx].ultimo_mensaje = time(NULL);
        ok = 1;
    }
    pthread_mutex_unlock(&mutex_lista);

    return ok;
}

static int build_user_list(char *out, size_t out_size) {
    size_t used = 0;
    int count = 0;

    if (out_size == 0) {
        return 0;
    }
    out[0] = '\0';

    pthread_mutex_lock(&mutex_lista);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (!lista[i].activo) {
            continue;
        }

        int written = snprintf(out + used, out_size - used, "%s,%s;",
                               lista[i].username, lista[i].status);
        if (written < 0 || (size_t)written >= out_size - used) {
            break;
        }

        used += (size_t)written;
        count++;
    }
    pthread_mutex_unlock(&mutex_lista);

    return count;
}

static int get_user_info(const char *username, char *out, size_t out_size) {
    int found = 0;

    pthread_mutex_lock(&mutex_lista);
    int idx = find_client_index_by_username(username);
    if (idx != -1) {
        snprintf(out, out_size, "%s,%s", lista[idx].ip, lista[idx].status);
        found = 1;
    }
    pthread_mutex_unlock(&mutex_lista);

    return found;
}

static int get_sockfd_by_username(const char *username) {
    int sockfd = -1;

    pthread_mutex_lock(&mutex_lista);
    int idx = find_client_index_by_username(username);
    if (idx != -1) {
        sockfd = lista[idx].sockfd;
    }
    pthread_mutex_unlock(&mutex_lista);

    return sockfd;
}

static void broadcast_chat_message(const char *sender, const char *payload) {
    int sockets[MAX_CLIENTS];
    int count = 0;

    pthread_mutex_lock(&mutex_lista);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (lista[i].activo) {
            sockets[count++] = lista[i].sockfd;
        }
    }
    pthread_mutex_unlock(&mutex_lista);

    for (int i = 0; i < count; i++) {
        send_packet(sockets[i], CMD_MSG, sender, "ALL", payload);
    }
}

static void broadcast_disconnected(const char *username, int except_sockfd) {
    int sockets[MAX_CLIENTS];
    int count = 0;

    pthread_mutex_lock(&mutex_lista);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (lista[i].activo && lista[i].sockfd != except_sockfd) {
            sockets[count++] = lista[i].sockfd;
        }
    }
    pthread_mutex_unlock(&mutex_lista);

    for (int i = 0; i < count; i++) {
        send_packet(sockets[i], CMD_DISCONNECTED, "SERVER", "ALL", username);
    }
}

static void *inactivity_monitor_thread(void *arg) {
    (void)arg;

    while (server_running) {
        sleep(1);

        int sockets[MAX_CLIENTS];
        int count = 0;
        time_t now = time(NULL);

        pthread_mutex_lock(&mutex_lista);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (!lista[i].activo) {
                continue;
            }

            if (strcmp(lista[i].status, STATUS_INACTIVO) != 0 &&
                difftime(now, lista[i].ultimo_mensaje) >= INACTIVITY_TIMEOUT) {
                safe_copy(lista[i].status, sizeof(lista[i].status), STATUS_INACTIVO);
                sockets[count++] = lista[i].sockfd;
                log_msg("INACTIVITY", "Usuario '%s' pasó a %s por timeout",
                        lista[i].username, STATUS_INACTIVO);
            }
        }
        pthread_mutex_unlock(&mutex_lista);

        for (int i = 0; i < count; i++) {
            send_packet(sockets[i], CMD_MSG, "SERVER", "",
                        "Tu status cambió a INACTIVE");
        }
    }

    return NULL;
}

static void *client_thread(void *arg) {
    int clientfd = *(int *)arg;
    free(arg);

    struct sockaddr_in peer_addr;
    socklen_t peer_len = sizeof(peer_addr);
    char client_ip[INET_ADDRSTRLEN] = {0};
    char username[32] = {0};
    int registered = 0;

    if (getpeername(clientfd, (struct sockaddr *)&peer_addr, &peer_len) == 0) {
        inet_ntop(AF_INET, &peer_addr.sin_addr, client_ip, sizeof(client_ip));
    } else {
        safe_copy(client_ip, sizeof(client_ip), "UNKNOWN");
    }

    ChatPacket pkt;
    int rc = recv_packet(clientfd, &pkt);
    if (rc <= 0) {
        log_msg("ERROR", "Cliente %s se desconectó antes de registrarse", client_ip);
        close(clientfd);
        return NULL;
    }

    log_msg("RECV", "Primer paquete desde %s: %s (%u)",
            client_ip, command_to_string(pkt.command), pkt.command);

    if (pkt.command != CMD_REGISTER) {
        send_packet(clientfd, CMD_ERROR, "SERVER", "", "Debes registrarte primero");
        log_msg("REGISTER", "Cliente %s intentó usar comando %s sin registrarse",
                client_ip, command_to_string(pkt.command));
        close(clientfd);
        return NULL;
    }

    if (pkt.sender[0] == '\0' || pkt.payload[0] == '\0' ||
        strcmp(pkt.sender, pkt.payload) != 0) {
        send_packet(clientfd, CMD_ERROR, "SERVER", pkt.sender, "Registro inválido");
        log_msg("REGISTER", "Registro inválido desde %s", client_ip);
        close(clientfd);
        return NULL;
    }

    int add_result = add_client(pkt.sender, client_ip, clientfd);
    if (add_result == -2) {
        send_packet(clientfd, CMD_ERROR, "SERVER", pkt.sender, "Usuario ya existe");
        log_msg("REGISTER", "Registro rechazado: usuario '%s' ya existe", pkt.sender);
        close(clientfd);
        return NULL;
    }

    if (add_result == -3) {
        send_packet(clientfd, CMD_ERROR, "SERVER", pkt.sender, "IP ya registrada");
        log_msg("REGISTER", "Registro rechazado: IP '%s' ya está registrada", client_ip);
        close(clientfd);
        return NULL;
    }

    if (add_result < 0) {
        send_packet(clientfd, CMD_ERROR, "SERVER", pkt.sender, "Servidor lleno");
        log_msg("REGISTER", "Registro rechazado: servidor lleno");
        close(clientfd);
        return NULL;
    }

    safe_copy(username, sizeof(username), pkt.sender);
    registered = 1;

    log_msg("REGISTER", "Usuario '%s' registrado desde %s", username, client_ip);

    char welcome[128];
    snprintf(welcome, sizeof(welcome), "Bienvenido %s", username);

    if (send_packet(clientfd, CMD_OK, "SERVER", username, welcome) < 0) {
        char removed_name[32] = {0};
        remove_client_by_sockfd(clientfd, removed_name, sizeof(removed_name));
        log_msg("ERROR", "No se pudo enviar bienvenida a '%s'", username);
        close(clientfd);
        return NULL;
    }

    while (server_running) {
        rc = recv_packet(clientfd, &pkt);
        if (rc <= 0) {
            log_msg("DISCONNECT", "Desconexión abrupta de '%s' (%s)", username, client_ip);
            break;
        }

        touch_client_activity(clientfd);

        log_msg("COMMAND", "Usuario '%s' envió %s (%u)",
                username, command_to_string(pkt.command), pkt.command);

        switch (pkt.command) {
            case CMD_BROADCAST:
                log_msg("BROADCAST", "%s: %s", username, pkt.payload);
                broadcast_chat_message(username, pkt.payload);
                break;

            case CMD_DIRECT: {
                int targetfd = get_sockfd_by_username(pkt.target);
                if (targetfd == -1) {
                    log_msg("DIRECT", "%s intentó escribir a '%s', pero no está conectado",
                            username, pkt.target);
                    send_packet(clientfd, CMD_ERROR, "SERVER", username,
                                "Destinatario no conectado");
                } else {
                    log_msg("DIRECT", "%s -> %s: %s", username, pkt.target, pkt.payload);
                    send_packet(targetfd, CMD_MSG, username, pkt.target, pkt.payload);
                }
                break;
            }

            case CMD_LIST: {
                char user_list[957];
                int total = build_user_list(user_list, sizeof(user_list));
                log_msg("LIST", "'%s' solicitó lista de usuarios (%d conectados)",
                        username, total);
                send_packet(clientfd, CMD_USER_LIST, "SERVER", username, user_list);
                break;
            }

            case CMD_INFO: {
                char info[957];
                log_msg("INFO", "'%s' solicitó información de '%s'", username, pkt.target);

                if (get_user_info(pkt.target, info, sizeof(info))) {
                    send_packet(clientfd, CMD_USER_INFO, "SERVER", pkt.target, info);
                } else {
                    send_packet(clientfd, CMD_ERROR, "SERVER", username,
                                "Usuario no conectado");
                }
                break;
            }

            case CMD_STATUS:
                if (!is_valid_status(pkt.payload)) {
                    log_msg("STATUS", "'%s' intentó cambiar a status inválido: %s",
                            username, pkt.payload);
                    send_packet(clientfd, CMD_ERROR, "SERVER", username, "Status inválido");
                } else {
                    set_client_status_by_sockfd(clientfd, pkt.payload);
                    log_msg("STATUS", "'%s' cambió a %s", username, pkt.payload);
                    send_packet(clientfd, CMD_OK, "SERVER", username, pkt.payload);
                }
                break;

            case CMD_LOGOUT:
                log_msg("LOGOUT", "Usuario '%s' cerró sesión", username);
                send_packet(clientfd, CMD_OK, "SERVER", username, "Logout exitoso");
                goto cleanup;

            default:
                log_msg("ERROR", "Comando no soportado de '%s': %u", username, pkt.command);
                send_packet(clientfd, CMD_ERROR, "SERVER", username, "Comando no soportado");
                break;
        }
    }

cleanup:
    if (registered) {
        char removed_name[32] = {0};
        if (remove_client_by_sockfd(clientfd, removed_name, sizeof(removed_name))) {
            log_msg("DISCONNECT", "Usuario '%s' removido de la lista", removed_name);
            broadcast_disconnected(removed_name, clientfd);
        }
    }

    close(clientfd);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Uso: %s <puerto>\n", argv[0]);
        return EXIT_FAILURE;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handle_sigint;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    sigaction(SIGINT, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    int port = atoi(argv[1]);
    if (port <= 0 || port > 65535) {
        fprintf(stderr, "Puerto inválido\n");
        return EXIT_FAILURE;
    }

    g_serverfd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_serverfd < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }

    int opt = 1;
    if (setsockopt(g_serverfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt");
        close(g_serverfd);
        g_serverfd = -1;
        return EXIT_FAILURE;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons((uint16_t)port);

    if (bind(g_serverfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        close(g_serverfd);
        g_serverfd = -1;
        return EXIT_FAILURE;
    }

    if (listen(g_serverfd, BACKLOG) < 0) {
        perror("listen");
        close(g_serverfd);
        g_serverfd = -1;
        return EXIT_FAILURE;
    }

    pthread_t monitor_tid;
    if (pthread_create(&monitor_tid, NULL, inactivity_monitor_thread, NULL) != 0) {
        perror("pthread_create monitor");
        close(g_serverfd);
        g_serverfd = -1;
        return EXIT_FAILURE;
    }
    pthread_detach(monitor_tid);

    log_msg("SERVER", "Servidor escuchando en puerto %d", port);

    while (server_running) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int clientfd = accept(g_serverfd, (struct sockaddr *)&client_addr, &client_len);
        if (clientfd < 0) {
            if (!server_running || errno == EINTR || errno == EBADF) {
                break;
            }
            perror("accept");
            continue;
        }

        char ip[INET_ADDRSTRLEN] = {0};
        inet_ntop(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));
        log_msg("CONNECTION", "Nueva conexión desde %s:%d",
                ip, ntohs(client_addr.sin_port));

        int *arg = malloc(sizeof(int));
        if (arg == NULL) {
            perror("malloc");
            close(clientfd);
            continue;
        }
        *arg = clientfd;

        pthread_t tid;
        if (pthread_create(&tid, NULL, client_thread, arg) != 0) {
            perror("pthread_create client");
            free(arg);
            close(clientfd);
            continue;
        }
        pthread_detach(tid);
    }

    if (g_serverfd != -1) {
        close(g_serverfd);
        g_serverfd = -1;
    }

    log_msg("SERVER", "Servidor detenido");
    return EXIT_SUCCESS;
}