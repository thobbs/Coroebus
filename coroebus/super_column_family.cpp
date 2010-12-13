#include <string>
#include <set>
#include <sstream>
#include <iostream>
#include <ctime>

#include "cassandra/Cassandra.h"
#include "coroebus/column_family.h"
#include "coroebus/super_column_family.h"

using namespace coroebus;
using namespace std;
using namespace boost;
using namespace org::apache::cassandra;


SuperColumnFamily::SuperColumnFamily(Connection *connection, const std::string &column_family):
        ColumnFamily(connection, column_family)
{
}

SuperColumnFamily::~SuperColumnFamily() {}

void SuperColumnFamily::insert(const string &key, const string &supercolumn, const string &subcolumn,
                          const string &value, int64_t timestamp, int32_t ttl,
                          ConsistencyLevel::type cl)
{
    ColumnParent *cp = new ColumnParent();
    cp->column_family = _column_family;
    cp->super_column = supercolumn;
    cp->__isset.super_column = true;

    if (timestamp == -1)
        timestamp = get_time();

    Column *column = new Column();
    column->name = subcolumn;
    column->value = value;
    column->timestamp = timestamp;
    column->ttl = ttl;

    _client->insert(key, *cp, *column, cl);
}

void SuperColumnFamily::insert(const string &key, const string &supercolumn,
                          const map<string, string> &subcolumns, int64_t timestamp, int32_t ttl,
                          ConsistencyLevel:: type cl)
{
    map<string, map<string, string> > supercolumns;
    supercolumns.insert(pair<string, map<string, string> >(supercolumn, subcolumns));
    return insert(key, supercolumns, timestamp, ttl, cl);
}

void SuperColumnFamily::insert(const string &key, const map<string, map<string, string> > &supercolumns,
                          int64_t timestamp, int32_t ttl, ConsistencyLevel::type cl)
{
    if (timestamp == -1)
        timestamp = get_time();

    vector<Mutation> mut_list;
    for(map<string, map<string, string> >::const_iterator it = supercolumns.begin();
        it != supercolumns.end();
        ++it)
    {
        SuperColumn *temp_super = new SuperColumn();
        temp_super->name = (*it).first;

        vector<Column> col_vector;
        map<string, string> subcolumns = (*it).second;

        for(map<string, string>::const_iterator col_it = subcolumns.begin();
            col_it != subcolumns.end();
            ++col_it)
        {
            Column *column = new Column();
            column->name = (*col_it).first;
            column->value = (*col_it).second;
            column->timestamp = timestamp;
            column->ttl = ttl;
            col_vector.push_back(*column);
        }
        temp_super->columns = col_vector;

        ColumnOrSuperColumn *cosc = new ColumnOrSuperColumn();
        cosc->super_column = *temp_super;
        cosc->__isset.super_column = true;

        Mutation *mut = new Mutation();
        mut->column_or_supercolumn = *cosc;
        mut->__isset.column_or_supercolumn = true;

        mut_list.push_back(*mut);
    }

    map<string, vector<Mutation> > innerMutMap;
    innerMutMap.insert(pair<string, vector<Mutation> >(_column_family, mut_list));

    map<string, map<string, vector<Mutation> > > mutationMap;
    mutationMap.insert(pair<string, map<string, vector<Mutation> > >(key, innerMutMap));

    _client->batch_mutate(mutationMap, cl);
}

map<string, map<string, string> >
SuperColumnFamily::get(const string &key, int32_t supercolumn_count, ConsistencyLevel::type cl)
{
    return get(key, "", "", supercolumn_count, cl);
}

map<string, map<string, string> >
SuperColumnFamily::get(const string &key, const vector<string> &supercolumns, ConsistencyLevel::type cl)
{
    SlicePredicate *sp = new SlicePredicate();
    sp->column_names = supercolumns;
    sp->__isset.column_names = true;
    return get(key, sp, cl);
}

map<string, map<string, string> >
SuperColumnFamily::get(const string &key, const string &supercolumn_start, const string &supercolumn_finish,
                  int32_t supercolumn_count, bool supercolumn_reversed, ConsistencyLevel::type cl)
{
    SlicePredicate *sp = make_slice_predicate(supercolumn_start, supercolumn_finish,
                                              supercolumn_count, supercolumn_reversed);
    return get(key, sp, cl);
}

/* private implementation */
map<string, map<string, string> >
SuperColumnFamily::get(const string &key, SlicePredicate *sp, ConsistencyLevel::type cl)
{
    ColumnParent *cp = new ColumnParent();
    cp->column_family = _column_family;

    vector<ColumnOrSuperColumn> results;
    _client->get_slice(results, key, *cp, *sp, cl);

    map<string, map<string, string> > supercolumns;
    for(vector<ColumnOrSuperColumn>::iterator it = results.begin(); it != results.end();++it) {
        SuperColumn scol = (*it).super_column;
        map<string, string> subcolumns;
        for(vector<Column>::iterator subcol_it = scol.columns.begin();
            subcol_it != scol.columns.end();
            ++subcol_it)
        {
            Column col = *subcol_it;
            subcolumns.insert(pair<string, string>(col.name, col.value));
        }
        supercolumns.insert(pair<string, map<string, string> >(scol.name, subcolumns));
    }
    return supercolumns;
}

map<string, string>
SuperColumnFamily::get_supercolumn(const string &key, const string &supercolumn, int32_t subcolumn_count,
                              ConsistencyLevel::type cl)
{
    return get_supercolumn(key, supercolumn, "", "", subcolumn_count, cl);
}

map<string, string>
SuperColumnFamily::get_supercolumn(const string &key, const string &supercolumn,
                              const vector<string> &subcolumns, CL cl)
{
    SlicePredicate *sp = new SlicePredicate();
    sp->column_names = subcolumns;
    sp->__isset.column_names = true;
    return get_supercolumn(key, supercolumn, sp, cl);
}

map<string, string>
SuperColumnFamily::get_supercolumn(const string &key, const string &supercolumn,
                              const string &subcolumn_start,
                              const string &subcolumn_finish,
                              int32_t subcolumn_count,
                              bool subcolumn_reversed,
                              CL cl)
{
    SlicePredicate *sp = make_slice_predicate(subcolumn_start, subcolumn_finish,
                                              subcolumn_count, subcolumn_reversed);
    return get_supercolumn(key, supercolumn, sp, cl);
}

/* private implementation */
map<string, string>
SuperColumnFamily::get_supercolumn(const string &key, const string &supercolumn, SlicePredicate *sp,
                              ConsistencyLevel::type cl)
{
    ColumnParent *cp = new ColumnParent();
    cp->column_family = _column_family;
    cp->super_column = supercolumn;
    cp->__isset.super_column = true;

    vector<ColumnOrSuperColumn> results;
    _client->get_slice(results, key, *cp, *sp, cl);

    SuperColumn sc = results.front().super_column; // only one element

    map<string, string> subcolumns;
    for(vector<Column>::iterator it = sc.columns.begin(); it != sc.columns.end(); ++it)
    {
        Column col = *it;
        subcolumns.insert(pair<string, string>(col.name, col.value));
    }
    return subcolumns;
}

map<string, map<string, map<string, string> > >
SuperColumnFamily::multiget(const vector<string> &keys, int32_t supercolumn_count,
                       ConsistencyLevel::type cl)
{
    return multiget(keys, "", "", supercolumn_count, false, cl);
}

map<string, map<string, map<string, string> > >
SuperColumnFamily::multiget(const vector<string> &keys, const vector<string> &columns,
                       ConsistencyLevel::type cl)
{
    SlicePredicate *sp = new SlicePredicate();
    sp->column_names = columns;
    sp->__isset.column_names = true;
    return multiget(keys, sp, cl);
}

map<string, map<string, map<string, string> > >
SuperColumnFamily::multiget(const vector<string> &keys,
                       const string &supercolumn_start, const string &supercolumn_finish,
                       int32_t supercolumn_count, bool supercolumn_reversed, ConsistencyLevel::type cl)
{

    SlicePredicate *sp = make_slice_predicate(supercolumn_start, supercolumn_finish,
                                              supercolumn_count, supercolumn_reversed);
    return multiget(keys, sp, cl);
}

map<string, map<string, map<string, string> > >
SuperColumnFamily::multiget(const vector<string> &keys, SlicePredicate *sp, ConsistencyLevel::type cl)
{
    ColumnParent *cp = new ColumnParent();
    cp->column_family = _column_family;

    map<string, vector<ColumnOrSuperColumn> > results;
    _client->multiget_slice(results, keys, *cp, *sp, cl);

    map<string, map<string, map<string, string> > > supercolumn_rows;
    for(map<string, vector<ColumnOrSuperColumn> >::iterator it = results.begin(); it != results.end(); ++it)
    {
        map<string, map<string, string> > supercol_map;
        string key = (*it).first;
        vector<ColumnOrSuperColumn> supercol_vector = (*it).second;
        for(vector<ColumnOrSuperColumn>::iterator supercol_it = supercol_vector.begin();
            supercol_it != supercol_vector.end();
            ++supercol_it)
        {
            SuperColumn supercolumn = (*supercol_it).super_column;
            map<string, string> subcol_map;
            for(vector<Column>::iterator col_it = supercolumn.columns.begin();
                col_it != supercolumn.columns.end();
                ++col_it)
            {
                Column col = (*col_it);
                subcol_map.insert(pair<string, string>(col.name, col.value));
            }
            supercol_map.insert(pair<string, map<string, string> >(supercolumn.name, subcol_map));
        }
        supercolumn_rows.insert(pair<string, map<string, map<string, string> > >(key, supercol_map));
    }

    return supercolumn_rows;
}

/* start multiget_supercolumn */

map<string, map<string, string> >
SuperColumnFamily::multiget_supercolumn(const vector<string> &keys,
        int32_t supercolumn_count, const string &supercolumn, CL cl)
{
    return multiget_supercolumn(keys, supercolumn, "", "", supercolumn_count, false, cl);
}

map<string, map<string, string> >
SuperColumnFamily::multiget_supercolumn(
        const vector<string> &keys, const string &supercolumn,
        const string &subcolumn_start, const string &subcolumn_finish,
        int32_t subcolumn_count, bool subcolumn_reversed, CL cl)
{
    SlicePredicate *sp = make_slice_predicate(subcolumn_start, subcolumn_finish,
                                              subcolumn_count, subcolumn_reversed);
    return multiget_supercolumn(keys, supercolumn, sp, cl);
}

map<string, map<string, string> >
SuperColumnFamily::multiget_supercolumn(const vector<string> &keys,
        const string &supercolumn, const vector<string> &subcolumns, CL cl)
{
    SlicePredicate *sp = new SlicePredicate();
    sp->column_names = subcolumns;
    sp->__isset.column_names = true;
    return multiget_supercolumn(keys, supercolumn, sp, cl);
}

map<string, map<string, string> >
SuperColumnFamily::multiget_supercolumn(const vector<string> &keys,
        const string &supercolumn, SlicePredicate *sp, CL cl)
{
    ColumnParent *cp = new ColumnParent();
    cp->column_family = _column_family;
    cp->super_column = supercolumn;
    cp->__isset.super_column = true;

    map<string, vector<ColumnOrSuperColumn> > results;
    _client->multiget_slice(results, keys, *cp, *sp, cl);

    map<string, map<string, string> > subcolumn_rows;
    for(map<string, vector<ColumnOrSuperColumn> >::iterator it = results.begin(); it != results.end(); ++it)
    {
        string key = (*it).first;
        // Get the first (and only) CoSC from the vector and grab its super_column
        SuperColumn supercol = (*it).second.front().super_column;
        map<string, string> subcol_map;
        for(vector<Column>::iterator col_it = supercol.columns.begin();
            col_it != supercol.columns.end();
            ++col_it)
        {
            Column col = (*col_it);
            subcol_map.insert(pair<string, string>(col.name, col.value));
        }
        subcolumn_rows.insert(pair<string, map<string, string> >(key, subcol_map));
    }

    return subcolumn_rows;
}

/* end multiget_supercolumn */

SlicePredicate * 
SuperColumnFamily::make_slice_predicate(const string &start, const string &finish,
                                        int32_t count, bool reversed)
{
    SlicePredicate *sp = new SlicePredicate();
    SliceRange *sr = make_slice_range(start, finish, count, reversed);
    sp->slice_range = *sr;
    sp->__isset.slice_range = true;
    return sp;
}

void SuperColumnFamily::remove(const string &key, int64_t timestamp, CL cl)
{
    remove(key, "", "", timestamp, cl);
}

void SuperColumnFamily::remove(const string &key, const string &supercolumn, int64_t timestamp, CL cl)
{
    remove(key, supercolumn, "", timestamp, cl);
}

void SuperColumnFamily::remove(const string &key, const string &supercolumn, const string &subcolumn,
                               int64_t timestamp, CL cl)
{
    if (timestamp == -1)
        timestamp = get_time();

    ColumnPath *cp = new ColumnPath();
    cp->column_family = _column_family;
    if (supercolumn != "") {
        cp->super_column = supercolumn;
        cp->__isset.super_column = true;
    }
    if (subcolumn != "") {
        cp->column = subcolumn;
        cp->__isset.column = true;
    }
    _client->remove(key, *cp, timestamp, cl);
}

void SuperColumnFamily::remove(const string &key, const vector<string> &supercolumns,
                               int64_t timestamp, CL cl)
{
    Deletion * deletion = new Deletion();
    if (!supercolumns.empty()) {
        SlicePredicate *sp = new SlicePredicate();
        sp->column_names = supercolumns;
        sp->__isset.column_names = true;
        deletion->predicate = *sp;
        deletion->__isset.predicate = true;
    }

    remove(key, deletion, timestamp, cl);
}

void SuperColumnFamily::remove(const string &key, const string &supercolumn,
                               const vector<string> &subcolumns, int64_t timestamp, CL cl)
{
    Deletion * deletion = new Deletion();
    if (!subcolumns.empty()) {
        SlicePredicate *sp = new SlicePredicate();
        sp->column_names = subcolumns;
        sp->__isset.column_names = true;
        deletion->predicate = *sp;
        deletion->__isset.predicate = true;
    }
    deletion->super_column = supercolumn;
    deletion->__isset.super_column = true;

    remove(key, deletion, timestamp, cl);
}

void SuperColumnFamily::remove(const string &key, Deletion * deletion, int64_t timestamp, CL cl)
{
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
