#ifndef MODULE_HPP
#define MODULE_HPP

#include "ruby.h"

#include <iostream>

#include <google/protobuf/io/zero_copy_stream_impl.h>

VALUE getMesosModule();

namespace mesos { namespace ruby {

template <typename T>
bool readRubyProtobuf(VALUE obj, T* t)
{
  VALUE res = rb_funcall(obj, rb_intern("serialize_to_string"), 0);
  // TODO: be a lot less fast-and-loose with types here. need some assertions.
  google::protobuf::io::ArrayInputStream stream(RSTRING_PTR(res), RSTRING_LEN(res));

  bool success = t->ParseFromZeroCopyStream(&stream);
  return success;
}

template <typename T>
VALUE createRubyProtobuf(const T& t, const char *typeName)
{
  VALUE type = rb_const_get(getMesosModule(), rb_intern(typeName));

  std::string str;
  if (!t.SerializeToString(&str)) {
    // TODO
  }

  VALUE val = rb_funcall(type, rb_intern("parse"), 1, rb_str_new(str.data(), str.size()));

  return val;
}

}}
#endif
