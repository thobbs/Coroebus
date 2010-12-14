#include <string>
#include <set>
#include <sstream>

#include <gtest/gtest.h>

#include <coroebus/connection.h>
#include <coroebus/column_family.h>
#include <coroebus/super_column_family.h>

using namespace std;
using namespace coroebus;
using namespace boost;
using namespace org::apache::cassandra;


TEST(SuperColumnFamily, TestEmpty)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    SuperColumnFamily * cf = new SuperColumnFamily(connection, "Super1");

    string key = "SuperColumnFamily.TestEmpty";
    map<string, map<string, string> > single_result = cf->get(key);
    ASSERT_EQ(single_result.size(), 0);

    vector<string> keys;
    keys.push_back(key);
    map<string, map<string, map<string, string> > > results = cf->multiget(keys);
    ASSERT_EQ(results[key].size(), 0);
}

TEST(SuperColumnFamily, SingleSubcolumnInsert)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    SuperColumnFamily * cf = new SuperColumnFamily(connection, "Super1");

    string key = "SuperColumnFamily.SingleSubcolumnInsert";

    cf->insert(key, "1", "sub1", "val1");

    map<string, map<string, string> > expected;
    map<string, string> subcolumns;
    subcolumns.insert(pair<string, string>("sub1", "val1"));
    expected.insert(pair<string, map<string, string> >("1", subcolumns));

    // get the whole row at once
    map<string, map<string, string> > results = cf->get(key);
    ASSERT_EQ(results.size(), 1);
    ASSERT_TRUE(results == expected);

    // cleanup
    cf->remove(key);
}

TEST(SuperColumnFamily, SingleSupercolumnInsert)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    SuperColumnFamily * cf = new SuperColumnFamily(connection, "Super1");

    string key = "SuperColumnFamily.SingleSupercolumnInsert";

    map<string, string> subcolumns;
    subcolumns.insert(pair<string, string>("sub1", "val1"));
    subcolumns.insert(pair<string, string>("sub2", "val2"));
    cf->insert(key, "1", subcolumns);

    map<string, map<string, string> > expected;
    expected.insert(pair<string, map<string, string> >("1", subcolumns));

    // get the whole row at once
    map<string, map<string, string> > results = cf->get(key);
    ASSERT_EQ(results.size(), 1);
    ASSERT_TRUE(results == expected);

    // cleanup
    cf->remove(key);
}

TEST(SuperColumnFamily, MultipleSupercolumnInsert)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    SuperColumnFamily * cf = new SuperColumnFamily(connection, "Super1");

    string key = "SuperColumnFamily.MultipleSupercolumnInsert";

    map<string, string> subcolumns1;
    subcolumns1.insert(pair<string, string>("sub1", "val1"));
    subcolumns1.insert(pair<string, string>("sub2", "val2"));
    map<string, map<string, string> > supercol1;
    supercol1.insert(pair<string, map<string, string> >("1", subcolumns1));
    cf->insert(key, supercol1);

    map<string, string> subcolumns2;
    subcolumns2.insert(pair<string, string>("sub3", "val3"));
    subcolumns2.insert(pair<string, string>("sub4", "val4"));
    map<string, map<string, string> > supercol2;
    supercol2.insert(pair<string, map<string, string> >("2", subcolumns2));
    cf->insert(key, supercol2);

    map<string, map<string, string> > expected;
    expected.insert(pair<string, map<string, string> >("1", subcolumns1));
    expected.insert(pair<string, map<string, string> >("2", subcolumns2));

    // get the whole row at once
    map<string, map<string, string> > results = cf->get(key);
    ASSERT_EQ(results.size(), 2);
    ASSERT_TRUE(results == expected);

    // cleanup
    cf->remove(key);
}

TEST(SuperColumnFamily, Get)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    SuperColumnFamily * cf = new SuperColumnFamily(connection, "Super1");

    string key = "SuperColumnFamily.Get";

    cf->insert(key, "1", "sub1", "val1");
    cf->insert(key, "1", "sub2", "val2");
    cf->insert(key, "2", "sub3", "val3");
    cf->insert(key, "2", "sub4", "val4");

    // specify a set of column names
    vector<string> colnames;
    colnames.push_back("1");
    map<string, map<string, string> > results = cf->get(key, colnames);
    ASSERT_EQ(results.size(), 1);
    map<string, map<string, string> >::iterator it = results.begin();
    pair<string, map<string, string> > supercolumn = *it;
    ASSERT_EQ(supercolumn.first, "1");

    map<string, string> expected_subs;
    expected_subs["sub1"] = "val1";
    expected_subs["sub2"] = "val2";
    ASSERT_TRUE(supercolumn.second == expected_subs);

    // get a slice
    results = cf->get(key, "", "", 1, false);
    ASSERT_EQ(results.size(), 1);
    supercolumn = *(results.begin());
    ASSERT_EQ(supercolumn.first, "1");
    ASSERT_TRUE(supercolumn.second == expected_subs);


    // cleanup
    cf->remove(key);
}

TEST(SuperColumnFamily, Multiget)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    SuperColumnFamily * cf = new SuperColumnFamily(connection, "Super1");

    string key1 = "SuperColumnFamily.TestMultiget1";
    string key2 = "SuperColumnFamily.TestMultiget2";

    map<string, map<string, string> > supers;
    map<string, string> subcolumns1;
    subcolumns1["sub1"] = "val1";
    subcolumns1["sub2"] = "val2";
    supers["1"] = subcolumns1;
    map<string, string> subcolumns2;
    subcolumns2["sub3"] = "val3";
    subcolumns2["sub4"] = "val4";
    supers["2"] = subcolumns2;

    cf->insert(key1, supers);
    cf->insert(key2, supers);

    map<string, map<string, map<string, string> > > expected;
    expected[key1] = supers;
    expected[key2] = supers;

    vector<string> keys;
    keys.push_back(key1);
    keys.push_back(key2);
    map<string, map<string, map<string, string> > > results;
    results = cf->multiget(keys);
    ASSERT_EQ(results.size(), 2);
    ASSERT_TRUE(results == expected);

    // specify a set of column names
    vector<string> colnames;
    colnames.push_back("1");
    results = cf->multiget(keys, colnames);

    expected = map<string, map<string, map<string, string> > >();
    map<string, map<string, string> > expected_super;
    expected_super["1"] = subcolumns1;
    expected[key1] = expected_super;
    expected[key2] = expected_super;

    ASSERT_EQ(results.size(), 2);
    ASSERT_TRUE(results == expected);

    // do a slice
    results = cf->multiget(keys, "", "", 1, false);
    ASSERT_EQ(results.size(), 2);
    ASSERT_TRUE(results == expected);

    // cleanup
    cf->remove(key1);
    cf->remove(key2);
}

TEST(SuperColumnFamily, GetSubcolumnCount)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    SuperColumnFamily * cf = new SuperColumnFamily(connection, "Super1");
    string key = "SuperColumnFamily.GetSubcolumnCount";

    map<string, map<string, string> > supercolumns;
    map<string, string> subcolumns;
    subcolumns["1"] = "val1";
    subcolumns["2"] = "val2";
    subcolumns["3"] = "val3";
    supercolumns["super"] = subcolumns;
    cf->insert(key, supercolumns);

    // count the whole supercolumn at once
    ASSERT_EQ(cf->get_subcolumn_count(key, "super"), 3);

    // specify a set of column names
    vector<string> names;
    names.push_back("1");
    names.push_back("2");
    ASSERT_EQ(cf->get_subcolumn_count(key, "super", names), 2);

    // do a slice
    ASSERT_EQ(cf->get_subcolumn_count(key, "super", "1", "2"), 2);

    // cleanup
    cf->remove(key);
}

TEST(SuperColumnFamily, MultigetSubcolumnCount)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    SuperColumnFamily * cf = new SuperColumnFamily(connection, "Super1");
    string key1 = "SuperColumnFamily.MultigetSubcolumnCount1";
    string key2 = "SuperColumnFamily.MultigetSubcolumnCount2";

    map<string, map<string, string> > supercolumns;
    map<string, string> subcolumns;
    subcolumns["1"] = "val1";
    subcolumns["2"] = "val2";
    subcolumns["3"] = "val3";
    supercolumns["super"] = subcolumns;
    cf->insert(key1, supercolumns);
    cf->insert(key2, supercolumns);

    vector<string> keys;
    keys.push_back(key1);
    keys.push_back(key2);

    // count the whole supercolumn at once
    map<string, int32_t> expected;
    expected[key1] = 3;
    expected[key2] = 3;

    map<string, int32_t> counts = cf->multiget_subcolumn_count(keys, "super");
    ASSERT_TRUE(counts == expected);

    // specify a set of column names
    vector<string> names;
    names.push_back("1");
    names.push_back("2");
    counts = cf->multiget_subcolumn_count(keys, "super", names);
    expected[key1] = 2;
    expected[key2] = 2;
    ASSERT_TRUE(counts == expected);

    // do a slice
    counts = cf->multiget_subcolumn_count(keys, "super", "1", "2");
    ASSERT_TRUE(counts == expected);

    // cleanup
    cf->remove(key1);
    cf->remove(key2);
}

TEST(SuperColumnFamily, Remove)
{
    Connection * connection = new Connection("Keyspace1", "localhost:9160");
    SuperColumnFamily * cf = new SuperColumnFamily(connection, "Super1");
    string key = "SuperColumnFamily.Remove";

    map<string, map<string, string> > supercolumns;
    map<string, string> subcolumns;
    subcolumns["1"] = "val1";
    subcolumns["2"] = "val2";
    subcolumns["3"] = "val3";
    supercolumns["super"] = subcolumns;
    cf->insert(key, supercolumns);
    ASSERT_EQ(cf->get_count(key), 1);

    cf->remove(key, "super", "1");
    map<string, map<string, string> > result = cf->get(key, "super");
    subcolumns.erase("1");
    ASSERT_TRUE(result["super"] == subcolumns);

    vector<string> subcolumn_names;
    subcolumn_names.push_back("2");
    cf->remove(key, "super", subcolumn_names);
    result = cf->get(key, "super");
    subcolumns.erase("2");
    ASSERT_TRUE(result["super"] == subcolumns);

    cf->remove(key, "super");
    ASSERT_EQ(cf->get_count(key), 0);
}
