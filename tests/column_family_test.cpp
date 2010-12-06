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


TEST(ColumnFamily, Basic)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    ColumnFamily * cf = new ColumnFamily(connection, "Standard1");
    map<string, string> columns;
    columns.insert(pair<string, string>("col", "val"));
    cf->insert("key1", columns);

    map<string, string> cols = cf->get("key1");
    for(map<string, string>::iterator it = cols.begin(); it != cols.end(); ++it) {
        cout << (*it).first << ": " << (*it).second << endl;
    }

    columns.insert(pair<string, string>("col2", "val2"));
    cf->insert("key2", columns);

    cols = cf->get("key2");
    for(map<string, string>::iterator it = cols.begin(); it != cols.end(); ++it) {
        cout << (*it).first << ": " << (*it).second << endl;
    }

    cf->get_count("key2");

    vector<string> keys;
    keys.push_back("key1");
    keys.push_back("key2");
    map<string, map<string, string> > rows = cf->multiget(keys);
    map<string, int32_t> counts = cf->multiget_count(keys);

    vector<string> rm_cols;
    rm_cols.push_back("col");
    rm_cols.push_back("col2");
    cf->remove("key1");
    cf->remove("key2", rm_cols);
}
