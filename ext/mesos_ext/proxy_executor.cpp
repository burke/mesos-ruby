/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ruby.h"

#include <iostream>

#include "module.hpp"
#include "proxy_executor.hpp"
#include "mesos_executor_driver_impl.hpp"

using namespace mesos;

using std::cerr;
using std::endl;
using std::string;
using std::vector;
using std::map;

#define UNWRAP_DATA executor_driver_info* data; \
  Data_Get_Struct(impl, executor_driver_info, data);


// This is profoundly lazy, but the driver, afaict, will only ever be
// executing one event at a time.
//
// Future Refactoring Candidate™.
static ExecutorInfo ei;
static FrameworkInfo fi;
static SlaveInfo si;
static TaskInfo ti;
static TaskID td;
static VALUE re;
static VALUE im;
static VALUE ax;

namespace mesos {
namespace ruby {

VALUE do_registered(int ignored)
{
  VALUE eio = createRubyProtobuf(ei, "ExecutorInfo");
  VALUE fio = createRubyProtobuf(fi, "FrameworkInfo");
  VALUE sio = createRubyProtobuf(si, "SlaveInfo");
  return rb_funcall(re, rb_intern("registered"), 4, im, eio, fio, sio);
}

void ProxyExecutor::registered(ExecutorDriver* driver,
                               const ExecutorInfo& executorInfo,
                               const FrameworkInfo& frameworkInfo,
                               const SlaveInfo& slaveInfo)
{ UNWRAP_DATA
  ei = executorInfo;
  fi = frameworkInfo;
  si = slaveInfo;
  re = data->ruby_executor;
  im = impl;
  rb_thread_create((VALUE (*)(...))do_registered, NULL);
}

VALUE do_reregistered(int ignored)
{
  VALUE sio = createRubyProtobuf(si, "SlaveInfo");
  return rb_funcall(re, rb_intern("reregistered"), 2, im, sio);
}

void ProxyExecutor::reregistered(ExecutorDriver* driver,
                                 const SlaveInfo& slaveInfo)
{ UNWRAP_DATA
  si = slaveInfo;
  re = data->ruby_executor;
  im = impl;
  rb_thread_create((VALUE (*)(...))do_reregistered, NULL);
}

VALUE do_disconnected(int ignored)
{
  return rb_funcall(re, rb_intern("disconnected"), 1, im);
}

void ProxyExecutor::disconnected(ExecutorDriver* driver)
{ UNWRAP_DATA
  re = data->ruby_executor;
  im = impl;
  rb_thread_create((VALUE (*)(...))do_disconnected, NULL);
}


VALUE do_launchTask(int ignored)
{
  VALUE tio = createRubyProtobuf(ti, "TaskInfo");
  return rb_funcall(re, rb_intern("launch_task"), 2, im, tio);
}

void ProxyExecutor::launchTask(ExecutorDriver* driver,
                               const TaskInfo& task)
{ UNWRAP_DATA
  re = data->ruby_executor;
  im = impl;
  ti = task;
  rb_thread_create((VALUE (*)(...))do_launchTask, NULL);
}

VALUE do_killTask(int ignored)
{
  VALUE tio = createRubyProtobuf(td, "TaskID");
  return rb_funcall(re, rb_intern("kill_task"), 2, im, tio);
}

void ProxyExecutor::killTask(ExecutorDriver* driver,
                             const TaskID& taskId)
{ UNWRAP_DATA
  re = data->ruby_executor;
  im = impl;
  td = taskId;
  rb_thread_create((VALUE (*)(...))do_killTask, NULL);
}

VALUE do_frameworkMessage(int meh) {
  return rb_funcall(re, rb_intern("framework_message"), 2, im, ax);
}

void ProxyExecutor::frameworkMessage(ExecutorDriver* driver,
                                     const string& str)
{ UNWRAP_DATA
  im = impl;
  re = data->ruby_executor;
  ax = rb_str_new(str.data(), str.length());
  rb_thread_create((VALUE (*)(...))do_frameworkMessage, NULL);
}

VALUE do_shutdown(int meh) {
  return rb_funcall(re, rb_intern("shutdown"), 1, im);
}

void ProxyExecutor::shutdown(ExecutorDriver* driver)
{ UNWRAP_DATA
  im = impl;
  re = data->ruby_executor;
  rb_thread_create((VALUE (*)(...))do_shutdown, NULL);
}

VALUE do_error(int meh) {
  return rb_funcall(re, rb_intern("error"), 2, im, ax);
}

void ProxyExecutor::error(ExecutorDriver* driver, const string& message)
{ UNWRAP_DATA
  im = impl;
  re = data->ruby_executor;
  ax = rb_str_new(message.data(), message.length());
  rb_thread_create((VALUE (*)(...))do_error, NULL);
}

} // namespace ruby {
} // namespace mesos {
