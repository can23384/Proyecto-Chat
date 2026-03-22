#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "protocolo.h"

static int sockfd = -1;
static volatile int running = 1;
static char my_username[32] = {0};
static char my_status[16] = STATUS_ACTIVO;

static ssize_t send_all(int fd, const void *buf, size_t len) {
    size_t total = 0;
    const char *p = (const char *)buf;

    while (total < len) {
        ssize_t n = send(fd, p + total, len - total, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            return -1;
        }
        total += (size_t)n;
    }

    return (ssize_t)total;
}

static ssize_t recv_all(int fd, void *buf, size_t len) {
    size_t total = 0;
    char *p = (char *)buf;

    while (total < len) {
        ssize_t n = recv(fd, p + total, len - total, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            return 0;
        }
        total += (size_t)n;
    }

    return (ssize_t)total;
}

static void packet_zero(ChatPacket *pkt) {
    memset(pkt, 0, sizeof(*pkt));
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

static size_t bounded_strlen(const char *s, size_t max_len) {
    size_t i = 0;
    while (i < max_len && s[i] != '\0') {
        i++;
    }
    return i;
}

static int send_packet(uint8_t command, const char *sender,
                       const char *target, const char *payload) {
    ChatPacket pkt;
    packet_zero(&pkt);

    pkt.command = command;
    safe_copy(pkt.sender, sizeof(pkt.sender), sender);
    safe_copy(pkt.target, sizeof(pkt.target), target);
    safe_copy(pkt.payload, sizeof(pkt.payload), payload);
    pkt.payload_len = (uint16_t)bounded_strlen(pkt.payload, sizeof(pkt.payload));

    return (send_all(sockfd, &pkt, sizeof(pkt)) == sizeof(pkt)) ? 0 : -1;
}

static int recv_packet(ChatPacket *pkt) {
    packet_zero(pkt);

    ssize_t n = recv_all(sockfd, pkt, sizeof(*pkt));
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

static void trim_newline(char *s) {
    size_t len = strlen(s);
    while (len > 0 && (s[len - 1] == '\n' || s[len - 1] == '\r')) {
        s[len - 1] = '\0';
        len--;
    }
}

static int starts_with(const char *text, const char *prefix) {
    return strncmp(text, prefix, strlen(prefix)) == 0;
}

static int is_valid_status(const char *status) {
    return strcmp(status, STATUS_ACTIVO) == 0 ||
           strcmp(status, STATUS_OCUPADO) == 0 ||
           strcmp(status, STATUS_INACTIVO) == 0;
}

static void print_help(void) {
    printf("\n=== Comandos ===\n");
    printf("/broadcast <mensaje>\n");
    printf("/msg <usuario> <mensaje>\n");
    printf("/status <ACTIVE|BUSY|INACTIVE>\n");
    printf("/list\n");
    printf("/info <usuario>\n");
    printf("/help\n");
    printf("/exit\n\n");
}

static void print_user_list(const char *payload) {
    char buffer[957];
    safe_copy(buffer, sizeof(buffer), payload);

    printf("\n=== Usuarios conectados ===\n");

    char *saveptr1 = NULL;
    char *entry = strtok_r(buffer, ";", &saveptr1);
    if (entry == NULL) {
        printf("(sin usuarios)\n");
    }

    while (entry != NULL) {
        char *comma = strchr(entry, ',');
        if (comma != NULL) {
            *comma = '\0';
            printf("- %s [%s]\n", entry, comma + 1);
        } else {
            printf("- %s\n", entry);
        }
        entry = strtok_r(NULL, ";", &saveptr1);
    }

    printf("\n");
}

static void print_user_info(const char *username, const char *payload) {
    char buffer[957];
    safe_copy(buffer, sizeof(buffer), payload);

    char *comma = strchr(buffer, ',');
    if (comma != NULL) {
        *comma = '\0';
        printf("\n=== Info de %s ===\n", username);
        printf("IP: %s\n", buffer);
        printf("Status: %s\n\n", comma + 1);
    } else {
        printf("\nInfo de %s: %s\n\n", username, payload);
    }
}

static void *receiver_thread(void *arg) {
    (void)arg;

    while (running) {
        ChatPacket pkt;
        int rc = recv_packet(&pkt);
        if (rc <= 0) {
            printf("\n[INFO] Conexión cerrada por el servidor.\n");
            running = 0;
            break;
        }

        switch (pkt.command) {
            case CMD_OK:
                printf("\n[OK] %s\n", pkt.payload);

                if (is_valid_status(pkt.payload)) {
                    safe_copy(my_status, sizeof(my_status), pkt.payload);
                    printf("[STATUS ACTUAL] %s\n", my_status);
                }
                break;

            case CMD_ERROR:
                printf("\n[ERROR] %s\n", pkt.payload);
                break;

            case CMD_MSG:
                if (strcmp(pkt.sender, "SERVER") == 0) {
                    printf("\n[SERVER] %s\n", pkt.payload);

                    if (strstr(pkt.payload, "INACTIVE") != NULL) {
                        safe_copy(my_status, sizeof(my_status), STATUS_INACTIVO);
                        printf("[STATUS ACTUAL] %s\n", my_status);
                    }
                } else if (strcmp(pkt.target, "ALL") == 0) {
                    printf("\n[GENERAL] %s: %s\n", pkt.sender, pkt.payload);
                } else {
                    printf("\n[PRIVADO] %s -> %s: %s\n",
                           pkt.sender, pkt.target, pkt.payload);
                }
                break;

            case CMD_USER_LIST:
                print_user_list(pkt.payload);
                break;

            case CMD_USER_INFO:
                print_user_info(pkt.target[0] ? pkt.target : "usuario", pkt.payload);
                break;

            case CMD_DISCONNECTED:
                printf("\n[INFO] Usuario desconectado: %s\n", pkt.payload);
                break;

            default:
                printf("\n[INFO] Paquete recibido con comando %u\n", pkt.command);
                break;
        }

        printf("> ");
        fflush(stdout);
    }

    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Uso: %s <username> <IP_servidor> <puerto>\n", argv[0]);
        return EXIT_FAILURE;
    }

    safe_copy(my_username, sizeof(my_username), argv[1]);

    const char *server_ip = argv[2];
    int port = atoi(argv[3]);

    if (port <= 0 || port > 65535) {
        fprintf(stderr, "Puerto inválido\n");
        return EXIT_FAILURE;
    }

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons((uint16_t)port);

    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        fprintf(stderr, "IP inválida: %s\n", server_ip);
        close(sockfd);
        return EXIT_FAILURE;
    }

    if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        close(sockfd);
        return EXIT_FAILURE;
    }

    if (send_packet(CMD_REGISTER, my_username, "", my_username) < 0) {
        perror("send register");
        close(sockfd);
        return EXIT_FAILURE;
    }

    ChatPacket response;
    int rc = recv_packet(&response);
    if (rc <= 0) {
        fprintf(stderr, "No se recibió respuesta del servidor\n");
        close(sockfd);
        return EXIT_FAILURE;
    }

    if (response.command == CMD_ERROR) {
        fprintf(stderr, "[ERROR] %s\n", response.payload);
        close(sockfd);
        return EXIT_FAILURE;
    }

    if (response.command != CMD_OK) {
        fprintf(stderr, "Respuesta inesperada del servidor\n");
        close(sockfd);
        return EXIT_FAILURE;
    }

    printf("[CONECTADO] %s\n", response.payload);
    printf("[STATUS ACTUAL] %s\n", my_status);
    print_help();

    pthread_t tid;
    if (pthread_create(&tid, NULL, receiver_thread, NULL) != 0) {
        perror("pthread_create");
        close(sockfd);
        return EXIT_FAILURE;
    }

    char line[1200];

    while (running) {
        printf("> ");
        fflush(stdout);

        if (fgets(line, sizeof(line), stdin) == NULL) {
            if (running) {
                send_packet(CMD_LOGOUT, my_username, "", "");
            }
            break;
        }

        trim_newline(line);

        if (line[0] == '\0') {
            continue;
        }

        if (strcmp(line, "/help") == 0) {
            print_help();
        } else if (strcmp(line, "/list") == 0) {
            if (send_packet(CMD_LIST, my_username, "", "") < 0) {
                perror("send list");
                break;
            }
        } else if (strcmp(line, "/exit") == 0) {
            if (send_packet(CMD_LOGOUT, my_username, "", "") < 0) {
                perror("send logout");
            }
            break;
        } else if (starts_with(line, "/broadcast ")) {
            const char *msg = line + 11;
            if (*msg == '\0') {
                printf("Uso: /broadcast <mensaje>\n");
                continue;
            }
            if (send_packet(CMD_BROADCAST, my_username, "", msg) < 0) {
                perror("send broadcast");
                break;
            }
        } else if (starts_with(line, "/msg ")) {
            char *rest = line + 5;
            while (*rest == ' ') {
                rest++;
            }

            char *space = strchr(rest, ' ');
            if (space == NULL) {
                printf("Uso: /msg <usuario> <mensaje>\n");
                continue;
            }

            *space = '\0';
            const char *target = rest;
            const char *msg = space + 1;

            if (*target == '\0' || *msg == '\0') {
                printf("Uso: /msg <usuario> <mensaje>\n");
                continue;
            }

            if (send_packet(CMD_DIRECT, my_username, target, msg) < 0) {
                perror("send direct");
                break;
            }
        } else if (starts_with(line, "/status ")) {
            const char *status = line + 8;

            if (!is_valid_status(status)) {
                printf("Status inválido. Usa ACTIVE, BUSY o INACTIVE\n");
                continue;
            }

            if (send_packet(CMD_STATUS, my_username, "", status) < 0) {
                perror("send status");
                break;
            }
        } else if (starts_with(line, "/info ")) {
            const char *target = line + 6;
            if (*target == '\0') {
                printf("Uso: /info <usuario>\n");
                continue;
            }

            if (send_packet(CMD_INFO, my_username, target, "") < 0) {
                perror("send info");
                break;
            }
        } else {
            printf("Comando no reconocido. Usa /help\n");
        }
    }

    running = 0;
    shutdown(sockfd, SHUT_RDWR);
    pthread_join(tid, NULL);
    close(sockfd);

    return EXIT_SUCCESS;
}