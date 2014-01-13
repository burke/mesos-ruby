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
#include "proxy_scheduler.hpp"
#include "mesos_scheduler_driver_impl.hpp"

using namespace mesos;

using std::cerr;
using std::endl;
using std::string;
using std::vector;
using std::map;

#define UNWRAP_DATA scheduler_driver_info* data; \
  Data_Get_Struct(impl, scheduler_driver_info, data);

#define SET_COMMON rs = data->ruby_scheduler; \
                        im = impl;

#define CALL_IN_THREAD(NAME) rb_thread_create((VALUE (*)(...)) NAME, NULL);

static VALUE rs;
static VALUE im;
static FrameworkID fd;
static ExecutorID ed;
static SlaveID sd;
static MasterInfo mi;
static OfferID od;
static TaskStatus ts;
static VALUE ax;
static vector<Offer> of;
static int st;

namespace mesos {
namespace ruby {

VALUE sdo_registered(int ignored)
{
  VALUE fid = createRubyProtobuf(fd, "FrameworkID");
  VALUE minfo = createRubyProtobuf(mi, "MasterInfo");
  return rb_funcall(rs, rb_intern("registered"), 3, im, fid, minfo);
}

void ProxyScheduler::registered(SchedulerDriver* driver,
                                const FrameworkID& frameworkId,
                                const MasterInfo& masterInfo)
{ UNWRAP_DATA ; SET_COMMON
  fd = frameworkId;
  mi = masterInfo;
  CALL_IN_THREAD(sdo_registered);
}

VALUE sdo_reregistered(int ignored)
{
  VALUE minfo = createRubyProtobuf(mi, "MasterInfo");
  return rb_funcall(rs, rb_intern("reregistered"), 2, im, minfo);
}

void ProxyScheduler::reregistered(SchedulerDriver* driver,
                                  const MasterInfo& masterInfo)
{ UNWRAP_DATA ; SET_COMMON
  mi = masterInfo;
  CALL_IN_THREAD(sdo_registered);
}

VALUE sdo_disconnected(int ignored)
{
  return rb_funcall(rs, rb_intern("disconnected"), 1, im);
}

void ProxyScheduler::disconnected(SchedulerDriver* driver)
{ UNWRAP_DATA ; SET_COMMON
  CALL_IN_THREAD(sdo_disconnected);
}

VALUE sdo_resourceOffers(int ignored)
{
  VALUE ary = rb_ary_new();

  for (size_t i = 0; i < of.size(); i++) {
    VALUE offer = createRubyProtobuf(of[i], "Offer");
    rb_ary_push(ary, offer);
  }

  return rb_funcall(rs, rb_intern("resource_offers"), 2, im, ary);
}

void ProxyScheduler::resourceOffers(SchedulerDriver* driver,
                                    const vector<Offer>& offers)
{ UNWRAP_DATA ; SET_COMMON
  of = offers;
  CALL_IN_THREAD(sdo_resourceOffers);
}

VALUE sdo_offerRescinded(int ignored)
{
  VALUE oid = createRubyProtobuf(od, "OfferID");
  return rb_funcall(rs, rb_intern("offer_rescinded"), 2, im, oid);
}

void ProxyScheduler::offerRescinded(SchedulerDriver* driver,
                                    const OfferID& offerId)
{ UNWRAP_DATA ; SET_COMMON
  od = offerId;
  CALL_IN_THREAD(sdo_offerRescinded);
}

VALUE sdo_statusUpdate(int ignored)
{
  VALUE stat = createRubyProtobuf(ts, "TaskStatus");
  return rb_funcall(rs, rb_intern("status_update"), 2, im, stat);
}

void ProxyScheduler::statusUpdate(SchedulerDriver* driver,
                                  const TaskStatus& status)
{ UNWRAP_DATA ; SET_COMMON
  ts = status;
  CALL_IN_THREAD(sdo_statusUpdate);
}

VALUE sdo_frameworkMessage(int ignored)
{
  VALUE eid = createRubyProtobuf(ed, "ExecutorID");
  VALUE sid = createRubyProtobuf(sd, "SlaveID");
  return rb_funcall(rs, rb_intern("framework_message"), 4, im, eid, sid, ax);
}

void ProxyScheduler::frameworkMessage(SchedulerDriver* driver,
                                      const ExecutorID& executorId,
                                      const SlaveID& slaveId,
                                      const string& str)
{ UNWRAP_DATA ; SET_COMMON
  ed = executorId;
  sd = slaveId;
  ax = rb_str_new(str.data(), str.length());
  CALL_IN_THREAD(sdo_frameworkMessage);
}

VALUE sdo_slaveLost(int ignored)
{
  VALUE sid = createRubyProtobuf(sd, "SlaveID");
  return rb_funcall(rs, rb_intern("slave_lost"), 2, im, sid);
}

void ProxyScheduler::slaveLost(SchedulerDriver* driver, const SlaveID& slaveId)
{ UNWRAP_DATA ; SET_COMMON
  sd = slaveId;
  CALL_IN_THREAD(sdo_slaveLost);
}

VALUE sdo_executorLost(int ignored)
{
  VALUE edo = createRubyProtobuf(ed, "ExecutorID");
  VALUE sdo = createRubyProtobuf(sd, "SlaveID");

  return rb_funcall(rs, rb_intern("executor_lost"), 4, im, edo, sdo, INT2FIX(st));
}

void ProxyScheduler::executorLost(SchedulerDriver* driver,
                                  const ExecutorID& executorId,
                                  const SlaveID& slaveId,
                                  int status)
{ UNWRAP_DATA ; SET_COMMON
  ed = executorId;
  sd = slaveId;
  st = status;
  CALL_IN_THREAD(sdo_executorLost);
}

VALUE sdo_error(int ignored)
{
  return rb_funcall(rs, rb_intern("error"), 2, im, ax);
}

void ProxyScheduler::error(SchedulerDriver* driver, const string& message)
{ UNWRAP_DATA ; SET_COMMON
  ax = rb_str_new(message.data(), message.length());
  CALL_IN_THREAD(sdo_error);
}

} // namespace python {
} // namespace mesos {
