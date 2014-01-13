require 'mkmf'
require 'pathname'

$INCFLAGS << " " << "-I#{File.expand_path "../mesos_headers", __FILE__}".quote

have_library('mesos')
create_makefile("mesos_ext")
