#include <string>
#include <set>
#include <sstream>
#include <iostream>
#include <ctime>

#include "cassandra/Cassandra.h"
#include "coroebus/column_family.h"

using namespace coroebus;
using namespace std;
using namespace boost;
using namespace org::apache::cassandra;


ColumnFamily::ColumnFamily(Connection *connection, const std::string &column_family):
        _client(connection->connect()), _column_family(column_family)
{
    this->_micros = 0;
}

ColumnFamily::~ColumnFamily() {}

void ColumnFamily::insert(const string &key, const map<string, string> &columns,
                          int64_t timestamp, int32_t ttl, CL cl)
{
    ColumnParent *cp = new ColumnParent();
    cp->column_family = _column_family;

    if (timestamp == -1)
        timestamp = get_time();

    if (columns.size() == 1)
    {
        map<string, string>::const_iterator it = columns.begin();
        Column *column = new Column();
        column->name = (*it).first;
        column->value = (*it).second;
        column->timestamp = timestamp;
        column->ttl = ttl;
        _client->insert(key, *cp, *column, cl);
    }
    else
    {
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
        innerMutMap.insert(pair<string, vector<Mutation> >(_column_family, mut_list));

        map<string, map<string, vector<Mutation> > > mutationMap;
        mutationMap.insert(pair<string, map<string, vector<Mutation> > >(key, innerMutMap));

        _client->batch_mutate(mutationMap, cl);
    }
}

int64_t ColumnFamily::get_time()
{
    time_t rawtime;
    time(&rawtime);
    int64_t timestamp(rawtime);
    timestamp *= 1000000;
    if (this->_micros == 1000000)
        this->_micros = 0;
    return timestamp + _micros++;
}

map<string, string>
ColumnFamily::get(const string &key, int32_t column_count, CL cl)
{
    return get(key, "", "", column_count, false, cl);
}

map<string, string>
ColumnFamily::get(const string &key, const vector<string> &columns, CL cl)
{
    SlicePredicate *sp = new SlicePredicate();
    sp->column_names = columns;
    sp->__isset.column_names = true;
    return get(key, sp, cl);
}

map<string, string>
ColumnFamily::get(const string &key, const string &column_start, const string &column_finish,
                  int32_t column_count, bool column_reversed, CL cl)
{
    SlicePredicate *sp = new SlicePredicate();
    SliceRange *sr = make_slice_range(column_start, column_finish, column_count, column_reversed);
    sp->slice_range = *sr;
    sp->__isset.slice_range = true;
    return get(key, sp, cl);
}

map<string, string>
ColumnFamily::get(const string &key, SlicePredicate *sp, CL cl)
{
    ColumnParent *cp = new ColumnParent();
    cp->column_family = _column_family;

    vector<ColumnOrSuperColumn> results;
    _client->get_slice(results, key, *cp, *sp, cl);

    map<string, string> cols;
    for(vector<ColumnOrSuperColumn>::iterator it = results.begin(); it != results.end();++it) {
        Column col = (*it).column;
        cols.insert(pair<string, string>(col.name, col.value));
    }
    return cols;
}

map<string, map<string, string> >
ColumnFamily::multiget(const vector<string> &keys, int32_t column_count,
                       CL cl)
{
    return multiget(keys, "", "", column_count, false, cl);
}

map<string, map<string, string> >
ColumnFamily::multiget(const vector<string> &keys, const vector<string> &columns, CL cl)
{
    SlicePredicate *sp = new SlicePredicate();
    sp->column_names = columns;
    sp->__isset.column_names = true;
    return multiget(keys, sp, cl);
}

map<string, map<string, string> >
ColumnFamily::multiget(const vector<string> &keys,
                       const string &column_start, const string &column_finish,
                       int32_t column_count, bool column_reversed, CL cl)
{

    SlicePredicate *sp = new SlicePredicate();
    SliceRange *sr = make_slice_range(column_start, column_finish, column_count, column_reversed);
    sp->slice_range = *sr;
    sp->__isset.slice_range = true;
    return multiget(keys, sp, cl);
}

map<string, map<string, string> >
ColumnFamily::multiget(const vector<string> &keys, SlicePredicate *sp, CL cl)
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
            row_columns.insert(pair<string, string>(col.name, col.value));
        }
        result_map.insert(pair<string, map<string, string> >(key, row_columns));
    }

    return result_map;
}

SliceRange* ColumnFamily::make_slice_range(const string &column_start, const string &column_finish,
                                          int32_t column_count, bool column_reversed)
{
    SliceRange *sr = new SliceRange();
    sr->start = column_start;
    sr->finish = column_finish;
    sr->count = column_count;
    sr->reversed = column_reversed;
    return sr;
}

int32_t ColumnFamily::get_count(const string &key, CL cl)
{
    return get_count(key, "", "", cl);
}

int32_t ColumnFamily::get_count(const string &key, const vector<string> &columns, CL cl)
{
    SlicePredicate *sp = new SlicePredicate();
    sp->column_names = columns;
    sp->__isset.column_names = true;
    return get_count(key, sp, cl);
}

int32_t ColumnFamily::get_count(const string &key, const string &column_start,
                                const string &column_finish, CL cl)
{
    SlicePredicate *sp = new SlicePredicate();
    SliceRange *sr = make_slice_range(column_start, column_finish, 2147483647);
    sp->slice_range = *sr;
    sp->__isset.slice_range = true;
    return get_count(key, sp, cl);
}

int32_t ColumnFamily::get_count(const string &key, SlicePredicate *sp, CL cl)
{
    ColumnParent *cp = new ColumnParent();
    cp->column_family = _column_family;
    return _client->get_count(key, *cp, *sp, cl);
}

map<string, int32_t>
ColumnFamily::multiget_count(const vector<string> &keys, CL cl)
{
    return multiget_count(keys, "", "", cl);
}

map<string, int32_t>
ColumnFamily::multiget_count(const vector<string> &keys, const vector<string> &columns, CL cl)
{
    SlicePredicate *sp = new SlicePredicate();
    sp->column_names = columns;
    sp->__isset.column_names = true;
    return multiget_count(keys, sp, cl);
}

map<string, int32_t>
ColumnFamily::multiget_count(const vector<string> &keys, const string &column_start,
                             const string &column_finish, CL cl)
{
    SlicePredicate *sp = new SlicePredicate();
    SliceRange *sr = make_slice_range(column_start, column_finish, 2147483647);
    sp->slice_range = *sr;
    sp->__isset.slice_range = true;
    return multiget_count(keys, sp, cl);
}

map<string, int32_t>
ColumnFamily::multiget_count(const vector<string> &keys, SlicePredicate *sp,
                             CL cl)
{
    ColumnParent *cp = new ColumnParent();
    cp->column_family = _column_family;
    map<string, int32_t> results;
    _client->multiget_count(results, keys, *cp, *sp, cl);
    return results;
}

void ColumnFamily::remove(const string &key, int64_t timestamp, CL cl)
{
    return remove(key, vector<string>(), timestamp, cl);
}

void ColumnFamily::remove(const string &key, const vector<string> &columns, int64_t timestamp, CL cl)
{
    Deletion * deletion = new Deletion();
    if (!columns.empty()) {
        SlicePredicate *sp = new SlicePredicate();
        sp->column_names = columns;
        sp->__isset.column_names = true;
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
    inner_mut_map.insert(pair<string, vector<Mutation> >(_column_family, mut_list));

    map<string, map<string, vector<Mutation> > > mutation_map;
    mutation_map.insert(pair<string, map<string, vector<Mutation> > >(key, inner_mut_map));

    _client->batch_mutate(mutation_map, cl);
}
