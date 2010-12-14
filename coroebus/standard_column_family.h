#ifndef __COROEBUS_STANDARD_COLUMN_FAMILY_H
#define __COROEBUS_STANDARD_COLUMN_FAMILY_H

#include <string>
#include <vector>
#include <tr1/memory>

#include "coroebus/connection.h"


namespace coroebus
{

class StandardColumnFamily: public ColumnFamily
{

typedef std::map<std::string, std::string> Subcols;
typedef std::map<std::string, Subcols > SubcolRows;

public:

    StandardColumnFamily(Connection *connection, const std::string &column_family);

    virtual ~StandardColumnFamily();

    virtual void insert(const std::string &key, const std::string &column,
                        const std::string &value, int64_t timestamp=-1, int32_t ttl=0,
                        CL cl = ONE);

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

    virtual void remove( const std::string &key, int64_t timestamp=-1,
        CL cl = ONE);

    virtual void remove( const std::string &key,
        const std::vector<std::string> &columns, int64_t timestamp=-1,
        CL cl = ONE);

private:

    virtual Subcols get(
        const std::string &key, org::apache::cassandra::SlicePredicate * sp, CL);

    virtual SubcolRows multiget(
        const std::vector<std::string> &keys, org::apache::cassandra::SlicePredicate * sp, CL);

};

} /* end namespace coroebus */

#endif /* __COROEBUS_STANDARD_COLUMN_FAMILY_H */
