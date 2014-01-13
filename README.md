# Mesos Ruby Drivers

NOTE: This project vendors a bunch of headers from Mesos 0.14.0 to prevent having
to build both projects in parallel. It is not-unlikely to break against newer
versions without reimporting these headers.

## Building

First, make sure `libmesos.so` (`libmesos-0.14.0.so`) is somewhere on your lib path.

    gem build mesos.gemspec
    gem install mesos-*.gem

## Using

Have a look at the files under [`examples`](https://github.com/Shopify/stack-scratch/tree/master/experiments/mesos/ruby-drivers/examples).

