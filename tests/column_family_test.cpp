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

#include <gtest/gtest.h>

#include <coroebus/connection.h>
#include <coroebus/column_family.h>

using namespace std;
using namespace coroebus;
using namespace boost;
using namespace org::apache::cassandra;


TEST(ColumnFamily, TestEmpty)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    ColumnFamily * cf = new ColumnFamily(connection, "Standard1");

    string key = "ColumnFamily.TestEmpty";
    map<string, string> single_result = cf->get(key);
    ASSERT_EQ(single_result.size(), 0);

    vector<string> keys;
    keys.push_back(key);
    map<string, map<string, string> > results = cf->multiget(keys);
    ASSERT_EQ(results[key].size(), 0);
}

TEST(ColumnFamily, TestGet)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    ColumnFamily * cf = new ColumnFamily(connection, "Standard1");

    string key = "ColumnFamily.TestGet";
    map<string, string> columns;
    columns.insert(pair<string, string>("1", "val1"));
    columns.insert(pair<string, string>("2", "val2"));
    cf->insert(key, columns);

    // get the whole row at once
    map<string, string> results = cf->get(key);
    ASSERT_EQ(results.size(), 2);
    ASSERT_TRUE(results == columns);

    // specify a set of column names
    vector<string> colnames;
    colnames.push_back("1");
    results = cf->get(key, colnames);
    ASSERT_EQ(results.size(), 1);
    map<string, string>::iterator it = results.begin();
    pair<string, string> column = *it;
    ASSERT_EQ(column.first, "1");
    ASSERT_EQ(column.second, "val1");

    // get a slice
    results = cf->get(key, "1");
    ASSERT_EQ(results.size(), 2);
    ASSERT_TRUE(results == columns);

    results = cf->get(key, "2");
    ASSERT_EQ(results.size(), 1);
    column = *(results.begin());
    ASSERT_EQ(column.first, "2");
    ASSERT_EQ(column.second, "val2");

    results = cf->get(key, "1", "", 1);
    ASSERT_EQ(results.size(), 1);
    column = *(results.begin());
    ASSERT_EQ(column.first, "1");
    ASSERT_EQ(column.second, "val1");

    results = cf->get(key, "", "", 1, true);
    ASSERT_EQ(results.size(), 1);
    column = *(results.begin());
    ASSERT_EQ(column.first, "2");
    ASSERT_EQ(column.second, "val2");

    // cleanup
    cf->remove(key);
}

TEST(ColumnFamily, TestMultiget)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    ColumnFamily * cf = new ColumnFamily(connection, "Standard1");

    string key1 = "ColumnFamily.TestMultiget1";
    string key2 = "ColumnFamily.TestMultiget2";

    map<string, string> columns1;
    columns1.insert(pair<string, string>("1", "val1"));
    columns1.insert(pair<string, string>("2", "val2"));
    cf->insert(key1, columns1);

    map<string, string> columns2;
    columns2.insert(pair<string, string>("1", "val1"));
    columns2.insert(pair<string, string>("2", "val2"));
    cf->insert(key2, columns2);

    map<string, map<string, string> > expected;
    expected.insert(pair<string, map<string, string> >(key1, columns1));
    expected.insert(pair<string, map<string, string> >(key2, columns2));

    vector<string> keys;
    keys.push_back(key1);
    keys.push_back(key2);
    map<string, map<string, string> > results;
    results = cf->multiget(keys);
    ASSERT_EQ(results.size(), 2);
    ASSERT_TRUE(results == expected);

    // cleanup
    cf->remove(key1);
    cf->remove(key2);
}
