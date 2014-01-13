#include "ruby.h"

#include "module.hpp"
#include "mesos_executor_driver_impl.hpp"
#include "mesos_scheduler_driver_impl.hpp"

static VALUE mMesos;

VALUE getMesosModule() {
	return mMesos;
}

extern "C" {
void Init_mesos_ext() {
  // we need to make sure the ruby portion is loaded before we go further.
  rb_f_require(rb_cObject, rb_str_new2("mesos"));
  mMesos = rb_const_get(rb_cObject, rb_intern("Mesos"));

	init_executor();
	init_scheduler();
}
}
