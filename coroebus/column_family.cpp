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
