/*
 * LibCassandra
 * Copyright (C) 2010 Padraig O'Sullivan
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#include <string>
#include <set>
#include <sstream>
#include <iostream>

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include "cassandra/Cassandra.h"
#include "coroebus/connection.h"

using namespace coroebus;
using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace org::apache::cassandra;
using namespace boost;


Connection::Connection(const std::string &keyspace,
                       const std::string &server,
                       const std::string &credentials,
                       bool framed_transport, double timeout):
               _keyspace(keyspace),
               _server(server),
               _credentials(credentials),
               _framed_transport(framed_transport),
               _timeout(timeout)
{
    _client_transport = NULL;
}

CassandraClient* Connection::connect() {
    /*
    std::cout << "keyspace: " << _keyspace << endl;
    std::cout << "server: " << _server << endl;
    std::cout << "credentials: " << _credentials << endl;
    std::cout << "framed_transport: " << _framed_transport << endl;
    std::cout << "timeout: " << _timeout << endl;
    std::cout << "client_trans: " << _client_transport << endl;
    */
    if (_client_transport == NULL) {
        _client_transport = new ClientTransport(_keyspace, _server, _credentials, _framed_transport, _timeout);
    }
    return _client_transport->client;
}

void Connection::close() {
    if (_client_transport != NULL) {
        _client_transport->transport->close();
    }
}

Connection::~Connection() {}

ClientTransport::ClientTransport(const std::string &keyspace,
                                 const std::string &server,
                                 const std::string &credentials,
                                 bool framed_transport,
                                 double timeout)
                    :
                    _keyspace(keyspace),
                    _server(server),
                    _credentials(credentials),
                    _framed_transport(framed_transport),
                    _timeout(timeout)
{
    string::size_type pos = server.find_first_of(':');
    _host = server.substr(0, pos);
    string tmp_port = server.substr(pos + 1);
    _port = atoi(tmp_port.c_str());

    boost::shared_ptr<TSocket> socket(new TSocket(_host, _port));
    socket->setSendTimeout(int(_timeout * 1000));
    socket->setRecvTimeout(int(_timeout * 1000));

    if (framed_transport)
        transport = boost::shared_ptr<TTransport>(new TFramedTransport(socket));
    else
        transport = boost::shared_ptr<TTransport>(new TBufferedTransport(socket));

    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    client = new CassandraClient(protocol);
    transport->open();

    client->set_keyspace(keyspace);

    // TODO - credentials
}

ClientTransport::~ClientTransport() {
    //transport->close();
}
