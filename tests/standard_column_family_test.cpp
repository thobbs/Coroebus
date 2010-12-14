#include <string>
#include <set>
#include <sstream>

#include <gtest/gtest.h>

#include <coroebus/connection.h>
#include <coroebus/column_family.h>
#include <coroebus/standard_column_family.h>

using namespace std;
using namespace coroebus;
using namespace boost;
using namespace org::apache::cassandra;


TEST(StandardColumnFamily, TestEmpty)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    StandardColumnFamily * cf = new StandardColumnFamily(connection, "Standard1");

    string key = "StandardColumnFamily.TestEmpty";
    map<string, string> single_result = cf->get(key);
    ASSERT_EQ(single_result.size(), 0);

    vector<string> keys;
    keys.push_back(key);
    map<string, map<string, string> > results = cf->multiget(keys);
    ASSERT_EQ(results[key].size(), 0);
}

TEST(StandardColumnFamily, Get)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    StandardColumnFamily * cf = new StandardColumnFamily(connection, "Standard1");

    string key = "StandardColumnFamily.TestGet";
    map<string, string> columns;
    columns["1"] = "val1";
    columns["2"] = "val2";
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

TEST(StandardColumnFamily, Multiget)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    StandardColumnFamily * cf = new StandardColumnFamily(connection, "Standard1");

    string key1 = "StandardColumnFamily.TestMultiget1";
    string key2 = "StandardColumnFamily.TestMultiget2";

    map<string, string> columns1;
    columns1["1"] = "val1";
    columns1["2"] = "val2";
    cf->insert(key1, columns1);

    map<string, string> columns2;
    columns2["1"] = "val1";
    columns2["2"] = "val2";
    cf->insert(key2, columns2);

    map<string, map<string, string> > expected;
    expected[key1] = columns1;
    expected[key2] = columns2;

    vector<string> keys;
    keys.push_back(key1);
    keys.push_back(key2);
    map<string, map<string, string> > results;
    results = cf->multiget(keys);
    ASSERT_EQ(results.size(), 2);
    ASSERT_TRUE(results == expected);

    // specify a set of column names
    vector<string> colnames;
    colnames.push_back("1");
    results = cf->multiget(keys, colnames);

    expected = map<string, map<string, string> >();
    map<string, string> expected_columns;
    expected_columns["1"] = "val1";
    expected[key1] = expected_columns;
    expected[key2] = expected_columns;

    ASSERT_EQ(results.size(), 2);
    ASSERT_TRUE(results == expected);

    // do a slice
    results = cf->multiget(keys, "1", "", 1);
    ASSERT_EQ(results.size(), 2);
    ASSERT_TRUE(results == expected);

    // cleanup
    cf->remove(key1);
    cf->remove(key2);
}

TEST(StandardColumnFamily, GetCount)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    StandardColumnFamily * cf = new StandardColumnFamily(connection, "Standard1");
    string key = "StandardColumnFamily.GetCount";

    map<string, string> columns;
    columns["1"] = "val1";
    columns["2"] = "val2";
    cf->insert(key, columns);

    ASSERT_EQ(cf->get_count(key), 2);
    ASSERT_EQ(cf->get_count(key, "", ""), 2);
    ASSERT_EQ(cf->get_count(key, "1", "2"), 2);

    vector<string> colnames;
    colnames.push_back("1");
    ASSERT_EQ(cf->get_count(key, colnames), 1);

    // cleanup
    cf->remove(key);
}

TEST(StandardColumnFamily, MultigetCount)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    StandardColumnFamily * cf = new StandardColumnFamily(connection, "Standard1");
    string key1 = "StandardColumnFamily.MultigetCount1";
    string key2 = "StandardColumnFamily.MultigetCount2";
    string key3 = "StandardColumnFamily.MultigetCount3";

    map<string, string> columns;
    columns["1"] = "val1";
    columns["2"] = "val2";
    cf->insert(key1, columns);
    cf->insert(key2, columns);

    columns = map<string, string>();
    columns["1"] = "val1";
    cf->insert(key3, columns);

    map<string, int32_t> expected;
    expected[key1] = 2;
    expected[key2] = 2;
    expected[key3] = 1;

    vector<string> keys;
    keys.push_back(key1);
    keys.push_back(key2);
    keys.push_back(key3);
    ASSERT_TRUE(cf->multiget_count(keys) == expected);

    // cleaup
    cf->remove(key1);
    cf->remove(key2);
    cf->remove(key3);
}

TEST(StandardColumnFamily, Remove)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    StandardColumnFamily * cf = new StandardColumnFamily(connection, "Standard1");
    string key = "StandardColumnFamily.Remove";

    map<string, string> columns;
    columns["1"] = "val1";
    columns["2"] = "val2";
    cf->insert(key, columns);
    ASSERT_EQ(cf->get_count(key), 2);

    vector<string> column_names;
    column_names.push_back("1");
    cf->remove(key, column_names);
    ASSERT_EQ(cf->get_count(key), 1);

    cf->remove(key);
    ASSERT_EQ(cf->get_count(key), 0);
}
