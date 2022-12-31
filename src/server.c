/** server.c
 * CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#define _DEFAULT_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>


#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"
#include "db_manager.h"
#include "query.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

extern Db* current_db;

Status shutdown_server() {
    log_info("shutdown_server1\n");
    struct Status ret_status = { OK, NULL };
    if (current_db) {
        release_db(&ret_status);
    }
    log_info("shutdown_server2\n");

    if (ret_status.code != OK) {
        log_info("saving db failed.");
        return ret_status;
    }

    ret_status.code = OK;
    return ret_status;
}

/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 *
 * Getting started hints:
 *      What are the structural attributes of a `query`?
 *      How will you interpret different queries?
 *      How will you ensure different queries invoke different execution paths in your code?
 **/
char* execute_DbOperator(DbOperator* query) {
    // there is a small memory leak here (when combined with other parts of your database.)
    // as practice with something like valgrind and to develop intuition on memory leaks, find and fix the memory leak. 
    if (!query) {
        return "Invalid DbOperator in execute_DbOperator()";
    }

    if (query->type == CREATE) {
        if (query->operator_fields.create_operator.create_type == _DB) {
            if (create_db(query->operator_fields.create_operator.name).code == OK) {
                log_info("Created db with OK status.\n");
                return "";
            } else {
                return "Failed Creat db";
            }
        } else if (query->operator_fields.create_operator.create_type == _TABLE) {
            Status create_status;
            create_table(query->operator_fields.create_operator.db,
                query->operator_fields.create_operator.name,
                query->operator_fields.create_operator.col_count,
                &create_status);
            if (create_status.code != OK) {
                log_info("adding a table failed.");
                return "Failed Create table";
            }
            log_info("Created table with OK status.\n");
            return "";
        } else if (query->operator_fields.create_operator.create_type == _COLUMN) {
            Status create_status;
            create_column(query->operator_fields.create_operator.table,
                query->operator_fields.create_operator.name,
                false,
                &create_status);
            if (create_status.code != OK) {
                log_info("adding a column failed.");
                return "Failed Create column";
            }
            log_info("Created column with OK status.\n");
            return "";
        } else if(query->operator_fields.create_operator.create_type == _INDEX){
            Status create_status;
            create_index(query->operator_fields.create_operator.column, 
                query->operator_fields.create_operator.clustered, 
                query->operator_fields.create_operator.sorted, 
                &create_status);
            if (create_status.code != OK) {
                log_info("adding a index failed.");
                return "Failed Create index";
            }
            log_info("Created index with OK status.\n");
            return "";
        }
    } else if (query->type == LOAD) {
        Status load_status;
        load_db(current_db, query->operator_fields.load_operator.file_name, &load_status);
        if (load_status.code != OK) {
            log_info("loading a db failed.");
            return "Failed Load db";
        }
        build_index(current_db);
        log_info("Load db with OK status.\n");
        return "";
    } else if (query->type == SHUTDOWN) {
        Status shutdown_status;
        shutdown_status = shutdown_server();
        if (shutdown_status.code != OK) {
            log_info("shutting down db failed.");
            return "Failed Shutdown";
        }
        log_info("Shutdown db with OK status.\n");
        return "shutdown";
    } else if (query->type == SELECT) {
        int low, high;
        int* low_pointer = &low;
        int* high_pointer = &high;
        Status select_status;
        Result* result;

        if (query->operator_fields.select_operator.has_low) {
            low = query->operator_fields.select_operator.low;
        } else {
            low_pointer = NULL;
        }

        if (query->operator_fields.select_operator.has_high) {
            high = query->operator_fields.select_operator.high;
        } else {
            high_pointer = NULL;
        }

        if (query->operator_fields.select_operator.select_type == COLUMN_SELECT) {
            result = select_column(
                query->operator_fields.select_operator.column,
                low_pointer,
                high_pointer,
                &select_status);
        } else {
            result = select_result(
                query->operator_fields.select_operator.col_result,
                query->operator_fields.select_operator.pos_result,
                low_pointer,
                high_pointer,
                &select_status);
        }

        if (select_status.code != OK) {
            log_info("select column failed.");
            return "Failed";
        }
        log_info("Select position list: ");

        int* position = result->payload;

        for (size_t i = 0; i < result->num_tuples; i++) {
            log_info("%d ", position[i]);
        }
        log_info("\n");

        add_result_to_context(query->context, query->operator_fields.select_operator.handle, result, &select_status);
        if (select_status.code != OK) {
            log_info("select column failed. Add result to context failed.");
            return "Failed Select";
        }
        log_info("Select column with OK status.\n");
        return "";
    } else if (query->type == FETCH) {
        Status fetch_status;
        Result* result;

        result = fetch_column(
            query->operator_fields.fetch_operator.column,
            query->operator_fields.fetch_operator.position,
            &fetch_status);

        if (fetch_status.code != OK) {
            log_info("fetch column failed.");
            return "Failed fetch column";
        }

        add_result_to_context(query->context, query->operator_fields.fetch_operator.handle, result, &fetch_status);
        if (fetch_status.code != OK) {
            log_info("fetch column failed. Add result to context failed.");
            return "Failed fetch column add result";
        }
        log_info("Fetch column with OK status.\n");
        return "";
    } else if (query->type == PRINT) {
        Status print_status;
        char* result_string;

        result_string = print(
            query->operator_fields.print_operator.results,
            query->operator_fields.print_operator.result_num,
            &print_status);
        // find_result_in_context(query->context, query->operator_fields.print_operator.handle, result, &print_status);
        if (print_status.code != OK) {
            log_info("print result failed.");
            return "Failed";
        }
        free(query->operator_fields.print_operator.results);
        log_info("Print result with OK status.\n");
        return result_string;
    } else if (query->type == AVG) {
        Status average_status;
        Result* result;

        result = average(
            query->operator_fields.average_operator.column,
            &average_status);

        if (average_status.code != OK) {
            log_info("Average operator failed.");
            return "Failed";
        }

        add_result_to_context(query->context, query->operator_fields.average_operator.handle, result, &average_status);
        if (average_status.code != OK) {
            log_info("Average operator failed. Add result to context failed.");
            return "Failed";
        }
        log_info("Average Operator finished with OK status.\n");
        return "";
    } else if (query->type == INSERT) {
        Status insert_status;
        insert_row(query->operator_fields.insert_operator.table, query->operator_fields.insert_operator.values, &insert_status);
        if (insert_status.code != OK) {
            log_info("relational insert a row failed.");
            return "Failed Load db";
        }
        log_info("Relational insert with OK status.\n");
        free(query->operator_fields.insert_operator.values);
        return "";
    } else if (query->type == SUM) {
        Status sum_status;
        Result* result;

        result = sum(
            &query->operator_fields.sum_operator.column,
            &sum_status);

        if (sum_status.code != OK) {
            log_info("Sum operator failed.");
            return "Failed";
        }

        add_result_to_context(query->context, query->operator_fields.sum_operator.handle, result, &sum_status);
        if (sum_status.code != OK) {
            log_info("Sum operator failed. Add result to context failed.");
            return "Failed";
        }
        log_info("Sum Operator finished with OK status.\n");
        return "";
    } else if (query->type == ADD) {
        Status add_status;
        Result* result;

        result = add(
            query->operator_fields.add_operator.column_one,
            query->operator_fields.add_operator.column_two,
            &add_status);

        if (add_status.code != OK) {
            log_info("Add operator failed.");
            return "Failed";
        }

        add_result_to_context(query->context, query->operator_fields.add_operator.handle, result, &add_status);
        if (add_status.code != OK) {
            log_info("Add operator failed. Add result to context failed.");
            return "Failed";
        }
        log_info("Add Operator finished with OK status.\n");
        return "";
    } else if (query->type == SUB) {
        Status sub_status;
        Result* result;

        result = sub(
            query->operator_fields.sub_operator.column_one,
            query->operator_fields.sub_operator.column_two,
            &sub_status);

        if (sub_status.code != OK) {
            log_info("Add operator failed.");
            return "Failed";
        }

        add_result_to_context(query->context, query->operator_fields.sub_operator.handle, result, &sub_status);
        if (sub_status.code != OK) {
            log_info("Add operator failed. Add result to context failed.");
            return "Failed";
        }
        log_info("Add Operator finished with OK status.\n");
        return "";
    } else if (query->type == MIN) {
        Status min_status;
        Result* result;

        result = min(
            query->operator_fields.min_operator.column,
            &min_status);

        if (min_status.code != OK) {
            log_info("Min operator failed.");
            return "Failed";
        }

        add_result_to_context(query->context, query->operator_fields.min_operator.handle, result, &min_status);
        if (min_status.code != OK) {
            log_info("Min operator failed. Add result to context failed.");
            return "Failed";
        }
        log_info("Min Operator finished with OK status.\n");
        return "";
    } else if (query->type == MAX) {
        Status max_status;
        Result* result;

        result = max(
            query->operator_fields.max_operator.column,
            &max_status);

        if (max_status.code != OK) {
            log_info("Max operator failed.");
            return "Failed";
        }

        add_result_to_context(query->context, query->operator_fields.max_operator.handle, result, &max_status);
        if (max_status.code != OK) {
            log_info("Max operator failed. Add result to context failed.");
            return "Failed";
        }
        log_info("Max Operator finished with OK status.\n");
        return "";
    } else if (query->type == BATCHED_SELECT) {
        Status ret_status;
        Result** results;
        int query_count = query->operator_fields.batched_select_operator.query_count;
        int curr_query = 0, curr_count = 0;

        while (curr_query < query_count) {
            if(query_count - curr_query > 150) {
                curr_count = 150;
            } else {
                curr_count = query_count - curr_query;
            }

            results = shared_select(
                query->operator_fields.batched_select_operator.operators + curr_query,
                curr_count,
                query->operator_fields.batched_select_operator.operators[0].column,
                &ret_status);

            if (ret_status.code != OK) {
                log_info("Batched select operator failed.");
                return "Failed";
            }

            for (int i = curr_query; i < curr_query + curr_count; i++) {
                add_result_to_context(query->context,
                    query->operator_fields.batched_select_operator.operators[i].handle,
                    results[i - curr_query],
                    &ret_status);
                if (ret_status.code != OK) {
                    log_info("Batched selct operator failed. Add result to context failed.");
                    return "Failed";
                }
            }

            curr_query = curr_query + curr_count;
        }
        
        log_info("Batched Selct Operator finished with OK status.\n");
        return "";
    } else if (query->type == JOIN) {
        Status join_status;
        Result** results;

        if (query->operator_fields.join_operator.type == NESTED_LOOP_JOIN) {
            results = nested_loop_join(
                query->operator_fields.join_operator.column_one,
                query->operator_fields.join_operator.position_one,
                query->operator_fields.join_operator.column_two,
                query->operator_fields.join_operator.position_two,
                &join_status);
        } else {
            results = hash_join(
                query->operator_fields.join_operator.column_one,
                query->operator_fields.join_operator.position_one,
                query->operator_fields.join_operator.column_two,
                query->operator_fields.join_operator.position_two,
                &join_status);
        }

        if (join_status.code != OK) {
            log_info("Join operator failed.");
            return "Failed";
        }

        add_result_to_context(query->context, query->operator_fields.join_operator.handle_one, results[0], &join_status);
        add_result_to_context(query->context, query->operator_fields.join_operator.handle_two, results[1], &join_status);
        if (join_status.code != OK) {
            log_info("Join operator failed. Join result to context failed.");
            return "Failed";
        }

        free(results);
        log_info("Join Operator finished with OK status.\n");
        return "";
    }

    return "165";
}


/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
int handle_client(int client_socket) {
    int done = 0;
    int length = 0;
    int shutdown = 0;
    char shutdown_message[] = "shutdown";

    log_info("Connected to socket: %d.\n", client_socket);
    log_info("handle_client 0\n");

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext* client_context = malloc(sizeof(ClientContext));
    client_context->chandle_table = malloc(sizeof(GeneralizedColumnHandle) * 10); // Change this to a more proper size
    client_context->chandle_slots = 10;
    client_context->chandles_in_use = 0;
    client_context->batching_query = 0;
    client_context->batch_operator = NULL;

    // Load db data to memory
    struct Status ret_status = { OK, NULL };
    log_info("handle_client 1\n");
    start_db(&ret_status);
    if (ret_status.code != OK) {
        log_err("loading existing db failed.\n");
        exit(1);
    }
    print_db();
    log_info("handle_client 2\n");

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response to the request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
            log_info("-- received length==0 message from client. done =1.");
        }

        if (!done) {
            char* result;
            char recv_buffer[recv_message.length + 1];
            length = recv(client_socket, recv_buffer, recv_message.length, 0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            // 1. Parse command
            //    Query string is converted into a request for an database operator
            log_info("about to parse command...\n");
            DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket, client_context);
            log_info("handle_client 3\n");
            if (!query && send_message.status == OK_DONE) {
                result = "";
            } else {
                // 2. Handle request
                //    Corresponding database operator is executed over the query
                // log_info("Before execute DBoperator 3\n");
                result = execute_DbOperator(query);
            }

            if (strcmp(result, shutdown_message) == 0) {
                done = 1;
                shutdown = 1;
                result = "";
                log_info("-- received shutdown command");
            }

            send_message.length = strlen(result);
            char send_buffer[send_message.length + 1];
            strcpy(send_buffer, result);
            send_message.payload = send_buffer;
            send_message.status = OK_WAIT_FOR_RESPONSE;

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            // 4. Send response to the request
            if (send(client_socket, result, send_message.length, 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            if (query && query->type == PRINT) {
                free(result);
            }

            if (query && query->type == BATCHED_SELECT) {
                free(client_context->batch_operator->operators);
                free(client_context->batch_operator);
                client_context->batching_query = 0;
            }

            free(query);
        }
        log_info("\n");
    } while (!done);

    free_client_context(client_context);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);

    return shutdown;
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr*)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You WILL need to extend this to handle MULTIPLE concurrent clients
// and remain running until it receives a shut-down command.
// 
// Getting Started Hints:
//      How will you extend main to handle multiple concurrent clients? 
//      Is there a maximum number of concurrent client connections you will allow?
//      What aspects of siloes or isolation are maintained in your design? (Think `what` is shared between `whom`?)
int main(void) {
    int shutdown = 0;
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    struct stat st = { 0 };

    if (stat("./database", &st) == -1) {
        if (mkdir("./database", 0777)) {
            char cwd[100];
            if (getcwd(cwd, sizeof(cwd)) != NULL) {
                printf("Current working dir: %s\n", cwd);
            } else {
                perror("getcwd() error");
                return 1;
            }
            log_info("When executing: mkdir(\"./database\")\n");
            perror("mkdir");
            exit(1);
        }
    }

    while (!shutdown) {
        log_info("Waiting for a connection %d ...\n", server_socket);

        struct sockaddr_un remote;
        socklen_t t = sizeof(remote);
        int client_socket = 0;

        if ((client_socket = accept(server_socket, (struct sockaddr*)&remote, &t)) == -1) {
            log_err("L%d: Failed to accept a new connection.\n", __LINE__);
            exit(1);
        }

        shutdown = handle_client(client_socket);
    }

    return 0;
}
