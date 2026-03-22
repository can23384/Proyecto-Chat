#ifndef PROTOCOLO_H
#define PROTOCOLO_H
#include <stdint.h>
/* ── Comandos Cliente → Servidor ── */
#define CMD_REGISTER 1
#define CMD_BROADCAST 2
#define CMD_DIRECT 3
#define CMD_LIST 4
#define CMD_INFO 5
#define CMD_STATUS 6
#define CMD_LOGOUT 7
/* ── Respuestas Servidor → Cliente ── */
#define CMD_OK 8
#define CMD_ERROR 9
#define CMD_MSG 10
#define CMD_USER_LIST 11
#define CMD_USER_INFO 12
#define CMD_DISCONNECTED 13
/* ── Valores de status ── */
#define STATUS_ACTIVO "ACTIVE"
#define STATUS_OCUPADO "BUSY"
#define STATUS_INACTIVO "INACTIVE"
/* ── Timeouts ── */
#define INACTIVITY_TIMEOUT 60 /* segundos sin actividad → INACTIVE */
/* ── Struct principal: siempre 1024 bytes ── */
typedef struct {
uint8_t command; /* 1 byte — operación a realizar */
uint16_t payload_len; /* 2 bytes — bytes válidos en payload */
char sender[32]; /* 32 bytes — nombre del remitente */
char target[32]; /* 32 bytes — destinatario (vacío=todos) */
char payload[957]; /* 957 bytes — contenido del mensaje */
} __attribute__((packed)) ChatPacket;
/* TOTAL: 1 + 2 + 32 + 32 + 957 = 1024 bytes */
#endif