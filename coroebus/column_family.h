#ifndef __COROEBUS_COLUMN_FAMILY_H
#define __COROEBUS_COLUMN_FAMILY_H

#include <string>
#include <vector>
#include <tr1/memory>

#include "coroebus/connection.h"


namespace coroebus
{

class ColumnFamily
{

typedef std::map<std::string, std::string> Subcols;
typedef std::map<std::string, Subcols > SubcolRows;

typedef org::apache::cassandra::ConsistencyLevel::type CL;

static const CL    ONE = org::apache::cassandra::ConsistencyLevel::ONE;
static const CL QUORUM = org::apache::cassandra::ConsistencyLevel::QUORUM;
static const CL    ALL = org::apache::cassandra::ConsistencyLevel::ALL;

public:

    ColumnFamily(Connection *connection, const std::string &column_family);

    virtual ~ColumnFamily();

    virtual void insert(const std::string &key, const Subcols &columns,
                        int64_t timestamp=-1, int32_t ttl=0, CL cl = ONE);

    virtual Subcols get(const std::string &key, int32_t count=100, CL cl = ONE);

    virtual Subcols get(const std::string &key, const std::vector<std::string> &columns, CL cl = ONE);

    virtual Subcols get(const std::string &key,
                        const std::string &column_start, const std::string &column_finish="",
                        int32_t column_count=100, bool column_reversed=false, CL cl = ONE);

    virtual SubcolRows multiget(const std::vector<std::string> &keys, int32_t column_count=100, CL cl = ONE);

    virtual SubcolRows multiget(const std::vector<std::string> &keys, const std::vector<std::string> &columns, CL cl = ONE);

    virtual SubcolRows multiget(const std::vector<std::string> &keys,
                                const std::string &column_start, const std::string &column_finish="",
                                int32_t column_count=100, bool column_reversed=false, CL cl = ONE);

    virtual int32_t get_count(const std::string &key, CL cl = ONE);

    virtual int32_t get_count(const std::string &key, const std::string &column_start,
                              const std::string &column_finish="", CL cl = ONE);

    virtual int32_t get_count(const std::string &key, const std::vector<std::string> &columns, CL cl = ONE);

    virtual std::map<std::string, int32_t> multiget_count(
        const std::vector<std::string> &keys,
        CL cl = ONE);

    virtual std::map<std::string, int32_t> multiget_count(
        const std::vector<std::string> &keys,
        const std::string &column_start,
        const std::string &column_finish="",
        CL cl = ONE);

    virtual std::map<std::string, int32_t> multiget_count(
        const std::vector<std::string> &keys,
        const std::vector<std::string> &columns,
        CL cl = ONE);

    virtual void remove( const std::string &key, int64_t timestamp=-1,
        CL cl = ONE);

    virtual void remove( const std::string &key,
        const std::vector<std::string> &columns, int64_t timestamp=-1,
        CL cl = ONE);

private:

    virtual int64_t get_time();

    virtual org::apache::cassandra::SliceRange * make_slice_range(
              const std::string &column_start="", const std::string &column_finish="",
              int32_t column_count=100, bool column_reversed=false);

    virtual Subcols get(
        const std::string &key, org::apache::cassandra::SlicePredicate * sp, CL);

    virtual SubcolRows multiget(
        const std::vector<std::string> &keys, org::apache::cassandra::SlicePredicate * sp, CL);

    virtual int32_t get_count(
        const std::string &key, org::apache::cassandra::SlicePredicate * sp, CL);

    virtual std::map<std::string, int32_t> multiget_count(
        const std::vector<std::string> &keys, org::apache::cassandra::SlicePredicate * sp, CL);

    org::apache::cassandra::CassandraClient * _client;
    std::string _column_family;
    int _micros;

};

} /* end namespace coroebus */

#endif /* __COROEBUS_COLUMN_FAMILY_H */
