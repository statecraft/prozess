
#ifndef topic_h
#define topic_h

#include "common.h"

struct topic_t;
typedef struct topic_t topic_t;

topic_t *db_new(char *topic_name);
void db_close(topic_t *db);

int db_send_hello(topic_t *topic, struct connection_t *conn);

bool db_key_in_conflict(topic_t *db, version_t updated_at_v, char *key, size_t keylen);
void db_mark_conflict_key(topic_t *db, version_t v, char *key, size_t keylen);

version_t db_append_event(topic_t *db, data_buffer event);

void db_subscribe(topic_t *topic, struct connection_t *connection, version_t start, size_t maxbytes);
void db_unsubscribe(topic_t *topic, struct connection_t *connection);

#endif
