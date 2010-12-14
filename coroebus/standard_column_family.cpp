#include <string>
#include <set>
#include <sstream>
#include <iostream>
#include <ctime>

#include "cassandra/Cassandra.h"
#include "coroebus/column_family.h"
#include "coroebus/standard_column_family.h"

using namespace coroebus;
using namespace std;
using namespace boost;
using namespace org::apache::cassandra;


StandardColumnFamily::StandardColumnFamily(Connection *connection, const std::string &column_family):
        ColumnFamily(connection, column_family)
{
    this->_micros = 0;
}

StandardColumnFamily::~StandardColumnFamily() {}

void StandardColumnFamily::insert(const string &key, const string &column, const string &value,
                                  int64_t timestamp, int32_t ttl, CL cl)
{
    ColumnParent *cp = new ColumnParent();
    cp->column_family = _column_family;

    if (timestamp == -1)
        timestamp = get_time();

    Column *col = new Column();
    col->name = column;
    col->value = value;
    col->timestamp = timestamp;
    col->ttl = ttl;
    _client->insert(key, *cp, *col, cl);
}

void StandardColumnFamily::insert(const string &key, const map<string, string> &columns,
                          int64_t timestamp, int32_t ttl, CL cl)
{
    ColumnParent *cp = new ColumnParent();
    cp->column_family = _column_family;

    if (timestamp == -1)
        timestamp = get_time();

    Column *temp_col = new Column();
    ColumnOrSuperColumn *temp_cosc = new ColumnOrSuperColumn();
    Mutation *temp_mut = new Mutation();
    vector<Mutation> mut_list;
    for(map<string, string>::const_iterator it = columns.begin(); it != columns.end(); ++it)
    {
        temp_col->name = (*it).first;
        temp_col->value = (*it).second;
        temp_col->timestamp = timestamp;
        temp_col->ttl = ttl;

        temp_cosc->column = *temp_col;
        temp_cosc->__isset.column = true;

        temp_mut->column_or_supercolumn = *temp_cosc;
        temp_mut->__isset.column_or_supercolumn = true;

        mut_list.push_back(*temp_mut);
    }
    map<string, vector<Mutation> > innerMutMap;
    innerMutMap[_column_family] = mut_list;

    map<string, map<string, vector<Mutation> > > mutationMap;
    mutationMap[key] = innerMutMap;

    _client->batch_mutate(mutationMap, cl);
}

map<string, string>
StandardColumnFamily::get(const string &key, int32_t column_count, CL cl)
{
    return get(key, "", "", column_count, false, cl);
}

map<string, string>
StandardColumnFamily::get(const string &key, const vector<string> &columns, CL cl)
{
    SlicePredicate *sp = make_slice_predicate(columns);
    return get(key, sp, cl);
}

map<string, string>
StandardColumnFamily::get(const string &key, const string &column_start, const string &column_finish,
                  int32_t column_count, bool column_reversed, CL cl)
{
    SlicePredicate *sp = make_slice_predicate(column_start, column_finish, column_count, column_reversed);
    return get(key, sp, cl);
}

map<string, string>
StandardColumnFamily::get(const string &key, SlicePredicate *sp, CL cl)
{
    ColumnParent *cp = new ColumnParent();
    cp->column_family = _column_family;

    vector<ColumnOrSuperColumn> results;
    _client->get_slice(results, key, *cp, *sp, cl);

    map<string, string> cols;
    for(vector<ColumnOrSuperColumn>::iterator it = results.begin(); it != results.end();++it) {
        Column col = (*it).column;
        cols[col.name] = col.value;
    }
    return cols;
}

map<string, map<string, string> >
StandardColumnFamily::multiget(const vector<string> &keys, int32_t column_count,
                       CL cl)
{
    return multiget(keys, "", "", column_count, false, cl);
}

map<string, map<string, string> >
StandardColumnFamily::multiget(const vector<string> &keys, const vector<string> &columns, CL cl)
{
    SlicePredicate *sp = make_slice_predicate(columns);
    return multiget(keys, sp, cl);
}

map<string, map<string, string> >
StandardColumnFamily::multiget(const vector<string> &keys,
                       const string &column_start, const string &column_finish,
                       int32_t column_count, bool column_reversed, CL cl)
{
    SlicePredicate *sp = make_slice_predicate(column_start, column_finish, column_count, column_reversed);
    return multiget(keys, sp, cl);
}

map<string, map<string, string> >
StandardColumnFamily::multiget(const vector<string> &keys, SlicePredicate *sp, CL cl)
{
    ColumnParent *cp = new ColumnParent();
    cp->column_family = _column_family;

    map<string, vector<ColumnOrSuperColumn> > results;
    _client->multiget_slice(results, keys, *cp, *sp, cl);

    map<string, map<string, string> > result_map;
    for(map<string, vector<ColumnOrSuperColumn> >::iterator it = results.begin(); it != results.end(); ++it)
    {
        map<string, string> row_columns;
        string key = (*it).first;
        vector<ColumnOrSuperColumn> col_vector = (*it).second;
        for(vector<ColumnOrSuperColumn>::iterator col_it = col_vector.begin(); col_it != col_vector.end(); ++col_it)
        {
            Column col = (*col_it).column;
            row_columns[col.name] = col.value;
        }
        result_map[key] = row_columns;
    }

    return result_map;
}

void StandardColumnFamily::remove(const string &key, int64_t timestamp, CL cl)
{
    return remove(key, vector<string>(), timestamp, cl);
}

void StandardColumnFamily::remove(const string &key, const vector<string> &columns, int64_t timestamp, CL cl)
{
    Deletion * deletion = new Deletion();
    if (!columns.empty()) {
        SlicePredicate *sp = make_slice_predicate(columns);
        deletion->predicate = *sp;
        deletion->__isset.predicate = true;
    }

    if (timestamp == -1)
        timestamp = get_time();
    deletion->timestamp =  timestamp;

    Mutation *mut = new Mutation();
    mut->deletion = *deletion;
    mut->__isset.deletion = true;

    vector<Mutation> mut_list;
    mut_list.push_back(*mut);

    map<string, vector<Mutation> > inner_mut_map;
    inner_mut_map[_column_family] = mut_list;

    map<string, map<string, vector<Mutation> > > mutation_map;
    mutation_map[key] = inner_mut_map;

    _client->batch_mutate(mutation_map, cl);
}
