#include "ruby.h"

#include <mesos/executor.hpp>
#include <string>
#include "module.hpp"
#include "mesos_executor_driver_impl.hpp"

#define UNWRAP_DATA executor_driver_info* data; \
  Data_Get_Struct(self, executor_driver_info, data);

using std::string;

extern "C" {

static VALUE cExecutor;

static VALUE exec_driver_impl_new(VALUE self, VALUE ruby_executor) {
  VALUE wrapped;
  executor_driver_info* x;

  wrapped = Data_Make_Struct(cExecutor, executor_driver_info, 0, free, x);
  rb_funcall(wrapped, rb_intern("initialize"), 1, ruby_executor);

  return wrapped;
}

static VALUE delete_driver(void * vdata) {
  executor_driver_info* data = (executor_driver_info *)vdata;
  delete data->driver;
  return Qnil;
}

static VALUE exec_driver_impl_finalize(VALUE executor_klass, VALUE self) { UNWRAP_DATA
  rb_thread_blocking_region(delete_driver, (void *)data, NULL, NULL);
  data->driver = NULL;
  delete data->proxy_executor;
  rb_gc_unregister_address(&data->ruby_executor);
  return Qnil;
}

static VALUE exec_driver_impl_init(VALUE self, VALUE ruby_executor) { UNWRAP_DATA
  data->ruby_executor = ruby_executor;
  rb_gc_register_address(&data->ruby_executor);
  data->proxy_executor = new mesos::ruby::ProxyExecutor(self);
  data->driver = new mesos::MesosExecutorDriver(data->proxy_executor);

  VALUE obspace = rb_const_get(rb_cObject, rb_intern("ObjectSpace"));
  VALUE meth = rb_funcall(cExecutor, rb_intern("method"), 1, rb_str_new2("finalize"));
  VALUE meth_proc = rb_funcall(meth, rb_intern("to_proc"), 0);
  rb_funcall(obspace, rb_intern("define_finalizer"), 2, self, meth_proc);

  return self;
}

static VALUE exec_start(VALUE self) { UNWRAP_DATA
  return INT2FIX(data->driver->start());
}

static VALUE exec_stop(VALUE self) { UNWRAP_DATA
  return INT2FIX(data->driver->stop());
}

static VALUE exec_abort(VALUE self) { UNWRAP_DATA
  return INT2FIX(data->driver->abort());
}

static VALUE join_driver(void* vdata) {
  executor_driver_info* data = (executor_driver_info *)vdata;
  return data->driver->join();
}

static VALUE run_driver(void* vdata) {
  executor_driver_info* data = (executor_driver_info *)vdata;
  return data->driver->run();
}

static VALUE exec_join(VALUE self) { UNWRAP_DATA
  VALUE val = rb_thread_blocking_region(join_driver, (void*)data, NULL, NULL);
  return INT2FIX(val);
}

static VALUE exec_run(VALUE self) { UNWRAP_DATA
  VALUE val = rb_thread_blocking_region(run_driver, (void*)data, NULL, NULL);
  return INT2FIX(val);
}

static VALUE exec_send_status_update(VALUE self, VALUE rproto) { UNWRAP_DATA
  mesos::TaskStatus taskStatus;
  mesos::ruby::readRubyProtobuf(rproto, &taskStatus);
  return INT2FIX(data->driver->sendStatusUpdate(taskStatus));
}

static VALUE exec_send_framework_message(VALUE self, VALUE str) { UNWRAP_DATA
  string cppstr = string(RSTRING_PTR(str), RSTRING_LEN(str));
  return INT2FIX(data->driver->sendFrameworkMessage(cppstr));
}


void init_executor() {
  cExecutor = rb_define_class_under(getMesosModule(), "MesosExecutorDriver", rb_cObject);

  rb_define_singleton_method(cExecutor, "new", RBFUNC(exec_driver_impl_new), 1);
  rb_define_singleton_method(cExecutor, "finalize", RBFUNC(exec_driver_impl_finalize), 1);
  rb_define_method(cExecutor, "initialize", RBFUNC(exec_driver_impl_init), 1);

  rb_define_method(cExecutor, "start", RBFUNC(exec_start), 0);
  rb_define_method(cExecutor, "stop", RBFUNC(exec_stop), 0);
  rb_define_method(cExecutor, "abort", RBFUNC(exec_abort), 0);
  rb_define_method(cExecutor, "join", RBFUNC(exec_join), 0);
  rb_define_method(cExecutor, "run", RBFUNC(exec_run), 0);
  rb_define_method(cExecutor, "send_status_update", RBFUNC(exec_send_status_update), 1);
  rb_define_method(cExecutor, "send_framework_message", RBFUNC(exec_send_framework_message), 1);
}


}
