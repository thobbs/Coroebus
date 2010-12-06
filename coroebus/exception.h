#ifndef __COROEBUS_EXCEPTION_H
#define __COROEBUS_EXCEPTION_H

#include <string>
#include <stdexcept>

namespace coroebus
{

class Exception : public std::runtime_error
{

public:

  Exception(const std::string &msg, int in_errno)
    :
      std::runtime_error(msg),
      _errno(in_errno)
  {}

  Exception(const char *msg, int in_errno)
    :
      std::runtime_error(std::string(msg)),
      _errno(in_errno)
  {}

  virtual ~Exception() throw() {}

  int getErrno() const
  {
    return _errno;
  }

private:

  int _errno;

};

class Error : public Exception
{

public:

  Error(const std::string &msg, int in_errno)
    :
      Exception(msg, in_errno)
  {}

  Error(const char *msg, int in_errno)
    :
      Exception(msg, in_errno)
  {}

  virtual ~Error() throw() {}
};

} /* end namespace coroebus */

#endif /* __COROEBUS_EXCEPTION_H */
