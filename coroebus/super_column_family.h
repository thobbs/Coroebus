#ifndef __COROEBUS_SUPER_COLUMN_FAMILY_H
#define __COROEBUS_SUPER_COLUMN_FAMILY_H

#include <string>
#include <vector>
#include <tr1/memory>

#include "coroebus/connection.h"
#include "coroebus/column_family.h"

namespace coroebus
{

class SuperColumnFamily: public ColumnFamily
{

typedef std::string Key;

typedef std::map<std::string, std::string> Subcol;
typedef std::map<std::string, std::string> Subcols;

typedef std::map<std::string, Subcols > SupercolRow;
typedef std::map<Key, SupercolRow > SupercolRows;
typedef std::map<Key, Subcols > SubcolRows;

public:

    SuperColumnFamily(Connection *connection, const std::string &column_family);

    virtual ~SuperColumnFamily();

    virtual void insert(
        const std::string &key,
        const std::string &supercolumn,
        const std::string &subcolumn,
        const std::string &value,
        int64_t timestamp=-1,
        int32_t ttl=0,
        CL cl = ONE);

    virtual void insert(
        const std::string &key,
        const std::string &supercolumn,
        const Subcols &subcolumns,
        int64_t timestamp=-1,
        int32_t ttl=0,
        CL cl = ONE);

    virtual void insert(
        const std::string &key,
        const std::map<std::string, Subcols > &supercolumns,
        int64_t timestamp=-1,
        int32_t ttl=0,
        CL cl = ONE);

    virtual SupercolRow get(const std::string &key, int32_t supercolumn_count=100, CL cl=ONE);

    virtual SupercolRow get(const std::string &key,
        const std::vector<std::string> &supercolumns,
        CL cl = ONE);

    virtual SupercolRow get(
        const std::string &key,
        const std::string &supercolumn_start,
        const std::string &supercolumn_finish="",
        int32_t supercolumn_count=100,
        bool supercolumn_reversed=false,
        CL cl = ONE);

    virtual Subcols get_supercolumn(
        const std::string &key,
        const std::string &supercolumn,
        int32_t subcolumn_count=100,
        CL cl = ONE);

    virtual Subcols get_supercolumn(
        const std::string &key,
        const std::string &supercolumn,
        const std::string &subcolumn_start,
        const std::string &subcolumn_finish="",
        int32_t subcolumn_count=100,
        bool subcolumn_reversed=false,
        CL cl = ONE);

    virtual Subcols get_supercolumn(
        const std::string &key,
        const std::string &supercolumn,
        const std::vector<std::string> &subcolumns,
        CL cl = ONE);

    virtual SupercolRows multiget( const std::vector<std::string> &keys,
        int32_t supercolumn_count=100, CL cl = ONE);

    virtual SupercolRows multiget( const std::vector<std::string> &keys,
        const std::vector<std::string> &supercolumns, CL cl = ONE);

    virtual SupercolRows multiget( const std::vector<std::string> &keys,
        const std::string &supercolumn_start, const std::string &supercolumn_finish="",
        int32_t supercolumn_count=100, bool supercolumn_reversed=false,
        CL cl = ONE);

    virtual SubcolRows multiget_supercolumn( const std::vector<std::string> &keys,
        int32_t supercolumn_count, const std::string &supercolumn, CL cl = ONE);

    virtual SubcolRows multiget_supercolumn(
        const std::vector<std::string> &keys,
        const std::string &supercolumn,
        const std::string &subcolumn_start,
        const std::string &subcolumn_finish="",
        int32_t subcolumn_count=100,
        bool subcolumn_reversed=false,
        CL cl = ONE);

    virtual SubcolRows multiget_supercolumn(
        const std::vector<std::string> &keys,
        const std::string &supercolumn,
        const std::vector<std::string> &subcolumns,
        CL cl = ONE);

    virtual int32_t get_subcolumn_count(const std::string &key, const std::string &supercolumn,
        CL cl = ONE);

    virtual int32_t get_subcolumn_count( const std::string &key, const std::string &supercolumn,
        const std::vector<std::string> &subcolumns, CL cl = ONE);

    virtual int32_t get_subcolumn_count(
        const std::string &key,
        const std::string &supercolumn,
        const std::string &subcolumn_start,
        const std::string &subcolumn_finish="",
        CL cl = ONE);

    virtual std::map<std::string, int32_t> multiget_subcolumn_count(
        const std::vector<std::string> &keys,
        const std::string &supercolumn,
        CL cl = ONE);

    virtual std::map<std::string, int32_t> multiget_subcolumn_count(
        const std::vector<std::string> &keys,
        const std::string &supercolumn,
        const std::vector<std::string> &subcolumns,
        CL cl = ONE);

    virtual std::map<std::string, int32_t> multiget_subcolumn_count(
        const std::vector<std::string> &keys,
        const std::string &supercolumn,
        const std::string &subcolumn_start,
        const std::string &subcolumn_finish="",
        CL cl = ONE);

    virtual void remove(const std::string &key, int64_t timestamp=-1, CL cl = ONE);

    virtual void remove(const std::string &key, const std::string &supercolumn,
        int64_t timestamp=-1, CL cl = ONE);

    virtual void remove(const std::string &key, const std::string &supercolumn,
        const std::string &subcolumn, int64_t timestamp=-1, CL cl = ONE);

    virtual void remove(const std::string &key, const std::vector<std::string> &supercolumns,
        int64_t timestamp=-1, CL cl = ONE);

    virtual void remove(const std::string &key, const std::string &supercolumn,
        const std::vector<std::string> &subcolumns, int64_t timestamp=-1,
        CL cl = ONE);

    virtual void remove(const std::string &key, org::apache::cassandra::Deletion * deletion,
        int64_t timstamp, CL cl);

private:

    virtual SupercolRow get(
        const std::string &key, org::apache::cassandra::SlicePredicate * sp, CL);

    virtual Subcols get_supercolumn( const std::string &key, const std::string &supercolumn,
        org::apache::cassandra::SlicePredicate * sp, CL);

    virtual SupercolRows multiget(const std::vector<std::string> &keys,
        org::apache::cassandra::SlicePredicate * sp, CL);

    virtual SubcolRows multiget_supercolumn(const std::vector<std::string> &keys,
        const std::string &supercolumn, org::apache::cassandra::SlicePredicate * sp, CL);

    virtual org::apache::cassandra::SlicePredicate * make_slice_predicate(
              const std::string &start, const std::string &finish,
              int32_t count, bool reversed);
};

} /* end namespace coroebus */

#endif /* __COROEBUS_SUPER_COLUMN_FAMILY_H */
