#include "ruby.h"

#include <mesos/scheduler.hpp>
#include <string>
#include "module.hpp"
#include "mesos_scheduler_driver_impl.hpp"

#define UNWRAP_DATA scheduler_driver_info* data; \
  Data_Get_Struct(self, scheduler_driver_info, data);

using std::string;
using std::vector;

extern "C" {

static VALUE cScheduler;

static VALUE sched_driver_impl_new(VALUE self, VALUE ruby_scheduler, VALUE ruby_framework, VALUE master) {
  VALUE wrapped;
  scheduler_driver_info* x;

  wrapped = Data_Make_Struct(cScheduler, scheduler_driver_info, 0, free, x);
  rb_funcall(wrapped, rb_intern("initialize"), 3, ruby_scheduler, ruby_framework, master);

  return wrapped;
}

static VALUE delete_driver(void * vdata) {
  scheduler_driver_info* data = (scheduler_driver_info *)vdata;
  delete data->driver;
  return Qnil;
}

static VALUE sched_driver_impl_finalize(VALUE scheduler_klass, VALUE self) { UNWRAP_DATA
  rb_thread_blocking_region(delete_driver, (void *)data, NULL, NULL);
  data->driver = NULL;
  delete data->proxy_scheduler;
  rb_gc_unregister_address(&data->ruby_scheduler);
  return Qnil;
}

static VALUE sched_driver_impl_init(VALUE self, VALUE ruby_scheduler, VALUE ruby_framework, VALUE master) { UNWRAP_DATA
  data->ruby_scheduler = ruby_scheduler;
  rb_gc_register_address(&data->ruby_scheduler);

  FrameworkInfo framework;
  mesos::ruby::readRubyProtobuf(ruby_framework, &framework);
  data->proxy_scheduler = new mesos::ruby::ProxyScheduler(self);

  data->driver = new mesos::MesosSchedulerDriver(data->proxy_scheduler, framework, RSTRING_PTR(master));

  VALUE obspace = rb_const_get(rb_cObject, rb_intern("ObjectSpace"));
  VALUE meth = rb_funcall(cScheduler, rb_intern("method"), 1, rb_str_new2("finalize"));
  VALUE meth_proc = rb_funcall(meth, rb_intern("to_proc"), 0);
  rb_funcall(obspace, rb_intern("define_finalizer"), 2, self, meth_proc);

  return self;
}

static VALUE sched_start(VALUE self) { UNWRAP_DATA
  return INT2FIX(data->driver->start());
}
static VALUE sched_stop(VALUE self) { UNWRAP_DATA
  return INT2FIX(data->driver->stop());
}
static VALUE sched_abort(VALUE self) { UNWRAP_DATA
  return INT2FIX(data->driver->abort());
}

static VALUE join_driver(void* vdata) {
  scheduler_driver_info* data = (scheduler_driver_info *)vdata;
  return data->driver->join();
}

static VALUE run_driver(void* vdata) {
  scheduler_driver_info* data = (scheduler_driver_info *)vdata;
  return data->driver->run();
}

static VALUE sched_join(VALUE self) { UNWRAP_DATA
  VALUE val = rb_thread_blocking_region(join_driver, (void*)data, NULL, NULL);
  return INT2FIX(val);
}

static VALUE sched_run(VALUE self) { UNWRAP_DATA
  VALUE val = rb_thread_blocking_region(run_driver, (void*)data, NULL, NULL);
  return INT2FIX(val);
}


static VALUE sched_request_resources(VALUE self, VALUE requests_obj) { UNWRAP_DATA
  vector<Request> requests;

  int len = RARRAY_LEN(requests_obj);
  for (int i = 0; i < len; i++) {
    VALUE request_obj = rb_ary_entry(requests_obj, i);
    Request request;
    mesos::ruby::readRubyProtobuf(request_obj, &request);
    requests.push_back(request);
  }

  return INT2FIX(data->driver->requestResources(requests));
}
static VALUE sched_launch_tasks(VALUE self, VALUE offer_id_obj, VALUE tasks_obj, VALUE filters_obj) { UNWRAP_DATA
  vector<TaskInfo> tasks;
  OfferID offerId;
  Filters filters;

  mesos::ruby::readRubyProtobuf(offer_id_obj, &offerId);

  int len = RARRAY_LEN(tasks_obj);
  for (int i = 0; i < len; i++) {
    VALUE task_obj = rb_ary_entry(tasks_obj, i);
    TaskInfo task;
    mesos::ruby::readRubyProtobuf(task_obj, &task);
    tasks.push_back(task);
  }

  mesos::ruby::readRubyProtobuf(filters_obj, &filters);

  return INT2FIX(data->driver->launchTasks(offerId, tasks, filters));
}
static VALUE sched_kill_task(VALUE self, VALUE tid_obj) { UNWRAP_DATA
  TaskID tid;
  mesos::ruby::readRubyProtobuf(tid_obj, &tid);
  return INT2FIX(data->driver->killTask(tid));
}
static VALUE sched_decline_offer(VALUE self, VALUE offer_id_obj, VALUE filters_obj) { UNWRAP_DATA
  OfferID offerId;
  Filters filters;

  mesos::ruby::readRubyProtobuf(offer_id_obj, &offerId);
  mesos::ruby::readRubyProtobuf(filters_obj, &filters);

  return INT2FIX(data->driver->declineOffer(offerId, filters));
}
static VALUE sched_revive_offers(VALUE self) { UNWRAP_DATA
  return INT2FIX(data->driver->reviveOffers());
}
static VALUE sched_send_framework_message(VALUE self, VALUE executor_id_obj, VALUE slave_id_obj, VALUE str) { UNWRAP_DATA
  ExecutorID executor_id;
  SlaveID slave_id;
  mesos::ruby::readRubyProtobuf(executor_id_obj, &executor_id);
  mesos::ruby::readRubyProtobuf(slave_id_obj, &slave_id);
  string cppstr = string(RSTRING_PTR(str), RSTRING_LEN(str));
  return INT2FIX(data->driver->sendFrameworkMessage(executor_id, slave_id, cppstr));
}

void init_scheduler() {

  cScheduler = rb_define_class_under(getMesosModule(), "MesosSchedulerDriver", rb_cObject);

  rb_define_singleton_method(cScheduler, "new", RBFUNC(sched_driver_impl_new), 3);
  rb_define_singleton_method(cScheduler, "finalize", RBFUNC(sched_driver_impl_finalize), 1);
  rb_define_method(cScheduler, "initialize", RBFUNC(sched_driver_impl_init), 3);

  rb_define_method(cScheduler, "start", RBFUNC(sched_start), 0);
  rb_define_method(cScheduler, "stop", RBFUNC(sched_stop), 0);
  rb_define_method(cScheduler, "abort", RBFUNC(sched_abort), 0);
  rb_define_method(cScheduler, "join", RBFUNC(sched_join), 0);
  rb_define_method(cScheduler, "run", RBFUNC(sched_run), 0);
  rb_define_method(cScheduler, "request_resources", RBFUNC(sched_request_resources), 1);
  rb_define_method(cScheduler, "launch_tasks", RBFUNC(sched_launch_tasks), 3);
  rb_define_method(cScheduler, "kill_task", RBFUNC(sched_kill_task), 1);
  rb_define_method(cScheduler, "decline_offer", RBFUNC(sched_decline_offer), 2);
  rb_define_method(cScheduler, "revive_offers", RBFUNC(sched_revive_offers), 0);
  rb_define_method(cScheduler, "send_framework_message", RBFUNC(sched_send_framework_message), 3);
}

}

