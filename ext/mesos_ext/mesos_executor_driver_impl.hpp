#ifndef RB_MESOS_EXECUTOR_DRIVER_IMPL_HPP
#define RB_MESOS_EXECUTOR_DRIVER_IMPL_HPP

#include "ruby.h"
#include <mesos/executor.hpp>


extern "C" {
void init_executor();
typedef struct _exectutor_driver_info executor_driver_info;
}

#include "proxy_executor.hpp"

#define RBFUNC(x) ((VALUE (*)(...))x)

using namespace mesos;

extern "C" {
typedef struct _exectutor_driver_info {
  MesosExecutorDriver* driver;
  ruby::ProxyExecutor* proxy_executor;
  VALUE ruby_executor;
} executor_driver_info;
}

#endif
