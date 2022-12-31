# Analyrical Database

## Introduction

This repository contains the implementation for a column-store database.


## How to Use
Inside `src` folder, you can build both the client and server using:

> `make all`

You should spin up the server first before trying to connect the client.

> `./server`
> `./client`

A high-level explanation of what happens is:

1. The server creates a socket to listen for an incoming connection.

2. The client attempts to connect to the server.

3. When the client has successfully connected, it waits for input from stdin.
Once received, it will create a message struct containing the input and
then send it to the server.  It immediately waits for a response to determine
if the server is willing to process the command.

1. When the server notices that a client has connected, it waits for a message
from the client.  When it receives the message from the client, it parses the
input and decides whether it is a valid/invalid query.
It immediately returns the response indicating whether it was valid or not.

1. Once the client receives the response, three things are possible:
   1) if the query was invalid, then just go back to waiting for input from stdin.
   2) if the query was valid and the server indicates that it will send back the
   result, then wait to receive another message from the server.
   3) if the query was valid but the server indicates that it will not send back
   the result, then go back to waiting for input on stdin.

2. Back on the server side, if the query is a valid query then it should
process it, and then send back the result if it was asked to.


