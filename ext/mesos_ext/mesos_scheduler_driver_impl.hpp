#ifndef RB_MESOS_SCHEDULER_DRIVER_IMPL_HPP
#define RB_MESOS_SCHEDULER_DRIVER_IMPL_HPP

#include "ruby.h"
#include <mesos/scheduler.hpp>

extern "C" {
void init_scheduler();
typedef struct _scheduler_driver_info scheduler_driver_info;
}

#include "proxy_scheduler.hpp"

#define RBFUNC(x) ((VALUE (*)(...))x)

using namespace mesos;

extern "C" {
typedef struct _scheduler_driver_info {
  MesosSchedulerDriver* driver;
  ruby::ProxyScheduler* proxy_scheduler;
  VALUE ruby_scheduler;
} scheduler_driver_info;
}

#endif

