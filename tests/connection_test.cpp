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

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include <gtest/gtest.h>

#include <cassandra/Cassandra.h>
#include <coroebus/connection.h>

using namespace std;
using namespace coroebus;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace org::apache::cassandra;
using namespace boost;


TEST(Connection, Basic)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    CassandraClient * client = connection->connect();

    KsDef ksdef;
    const std::string keyspace("Keyspace1");
    client->describe_keyspace(ksdef, keyspace);

    std::string cluster_name;
    client->describe_cluster_name(cluster_name);
    std::string expected("Test Cluster");
    EXPECT_EQ(cluster_name, expected);
}
