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

public:

    ColumnFamily(Connection *connection, const std::string &column_family);

    virtual ~ColumnFamily();

    virtual void insert(
        const std::string &key,
        const std::map<std::string, std::string> &columns,
        int64_t timestamp=-1,
        int32_t ttl=0,
        org::apache::cassandra::ConsistencyLevel::type cl=org::apache::cassandra::ConsistencyLevel::ONE);

    virtual std::map<std::string, std::string> get(
        const std::string &key,
        const std::vector<std::string> &columns=std::vector<std::string>(),
        const std::string &column_start="",
        const std::string &column_finish="",
        int32_t column_count=100,
        bool column_reversed=false,
        org::apache::cassandra::ConsistencyLevel::type cl=org::apache::cassandra::ConsistencyLevel::ONE);

    virtual std::map<std::string, std::map<std::string, std::string> > multiget(
        const std::vector<std::string> &keys,
        const std::vector<std::string> &columns=std::vector<std::string>(),
        const std::string &column_start="",
        const std::string &column_finish="",
        int32_t column_count=100,
        bool column_reversed=false,
        org::apache::cassandra::ConsistencyLevel::type cl=org::apache::cassandra::ConsistencyLevel::ONE);

    virtual int32_t get_count(
        const std::string &key,
        const std::vector<std::string> &columns=std::vector<std::string>(),
        const std::string &column_start="",
        const std::string &column_finish="",
        org::apache::cassandra::ConsistencyLevel::type cl=org::apache::cassandra::ConsistencyLevel::ONE);

    virtual std::map<std::string, int32_t> multiget_count(
        const std::vector<std::string> &keys,
        const std::vector<std::string> &columns=std::vector<std::string>(),
        const std::string &column_start="",
        const std::string &column_finish="",
        org::apache::cassandra::ConsistencyLevel::type cl=org::apache::cassandra::ConsistencyLevel::ONE);

    virtual void remove(
        const std::string &key,
        const std::vector<std::string> &columns=std::vector<std::string>(),
        int64_t timestamp=-1,
        org::apache::cassandra::ConsistencyLevel::type cl=org::apache::cassandra::ConsistencyLevel::ONE);


private:

    virtual int64_t get_time();

    virtual org::apache::cassandra::SliceRange * make_slice_range(
              const std::string &column_start="", const std::string &column_finish="",
              int32_t column_count=100, bool column_reversed=false);

    org::apache::cassandra::CassandraClient * _client;
    std::string _column_family;
    int _micros;

};

} /* end namespace coroebus */

#endif /* __COROEBUS_COLUMN_FAMILY_H */
