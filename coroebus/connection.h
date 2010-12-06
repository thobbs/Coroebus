#ifndef __COROEBUS_CONNECTION_H
#define __COROEBUS_CONNECTION_H

#include <string>
#include <vector>
#include <tr1/memory>

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include "cassandra/Cassandra.h"

namespace org { namespace apache { namespace cassandra { class CassandraClient; } } } 

namespace coroebus
{

class ClientTransport
{

public:

    ClientTransport(const std::string &keyspace,
                    const std::string &server,
                    const std::string &credentials,
                    bool framed_transport,
                    double timeout);

    virtual ~ClientTransport();

    org::apache::cassandra::CassandraClient *client;
    boost::shared_ptr<apache::thrift::transport::TTransport> transport;

private:

    std::string _keyspace;
    std::string _server;
    std::string _host;
    int _port;
    std::string _credentials;
    bool _framed_transport;
    double _timeout;

};

class Connection
{

public:

    Connection(const std::string &keyspace,
               const std::string &server = "localhost:9160",
               const std::string &credentials = "",
               bool framed_transport = true,
               double timeout = 0.5);

    virtual ~Connection();

    virtual org::apache::cassandra::CassandraClient* connect();
    virtual void close();

private:

    std::string _keyspace;
    std::string _server;
    std::string _host;
    std::string _credentials;
    int _port;
    bool _framed_transport;
    double _timeout;
    ClientTransport *_client_transport;
};
} /* end namespace coroebus */

#endif /* __COROEBUS_CONNECTION_H */
