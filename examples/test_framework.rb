#!/usr/bin/env ruby

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'rubygems'
require 'mesos'

TOTAL_TASKS = 5

TASK_CPUS = 1
TASK_MEM = 32

class ProtocolBuffers::Message
  def merge_from(msg)
    raise unless msg.class == self.class
    msg.fields.each do |i,field|
      f = msg.send(field.name)
      if f.kind_of?(ProtocolBuffers::Message)
        copy = f.class.new
        copy.merge_from(f)
        send("#{field.name}=", copy)
      else
        send("#{field.name}=", f)
      end
    end
  end
end

class TestScheduler < Mesos::Scheduler
  def initialize(executor)
    @executor = executor
    @taskData = {}
    @tasksLaunched = 0
    @tasksFinished = 0
    @messagesSent = 0
    @messagesReceived = 0
  end
  attr_accessor :executor, :taskData, :tasksLaunched, :tasksFinished, :messagesSent, :messagesReceived

  def registered(driver, frameworkId, masterInfo)
    puts "Registered with framework ID %s" % frameworkId.value
  end

  def resource_offers(driver, offers)
    puts "Got #{offers.size} resource offers"
    offers.each do |offer|
      tasks = []
      puts "Got resource offer %s" % offer.id.value
      if self.tasksLaunched < TOTAL_TASKS
        tid = self.tasksLaunched
        self.tasksLaunched += 1

        puts "Accepting offer on %s to start task %d" % [offer.hostname, tid]

        task = Mesos::TaskInfo.new
        task.task_id.value = tid.to_s
        task.slave_id.value = offer.slave_id.value
        task.name = "task %d" % tid
        task.executor.merge_from(self.executor)

        cpus = Mesos::Resource.new
        cpus.name = "cpus"
        cpus.type = Mesos::Value::Type::SCALAR
        cpus.scalar.value = TASK_CPUS
        task.resources << cpus

        mem = Mesos::Resource.new
        mem.name = "mem"
        mem.type = Mesos::Value::Type::SCALAR
        mem.scalar.value = TASK_MEM
        task.resources << mem

        tasks << task
        self.taskData[task.task_id.value] = [offer.slave_id, task.executor.executor_id]
      end
      driver.launch_tasks(offer.id, tasks, Mesos::Filters.new)
    end
  end

  def status_update(driver, update)
    puts "Task %s is in state %d" % [update.task_id.value, update.state]

    # Ensure the binary data came through.
    if update.data != "data with a \0 byte"
      puts "The update data did not match!"
      puts "  Expected: 'data with a \\x00 byte'"
      puts "  Actual:  #{ update.data.inspect}"
      exit(1)
    end

    if update.state == Mesos::TaskState::TASK_FINISHED
      self.tasksFinished += 1
      if self.tasksFinished == TOTAL_TASKS
        puts "All tasks done, waiting for final framework message"
      end

      slave_id, executor_id = self.taskData[update.task_id.value]

      self.messagesSent += 1
      driver.send_framework_message(executor_id, slave_id, "fw data with a \0 byte")
    end

  end

  def framework_message(driver, executorId, slaveId, message)
    self.messagesReceived += 1

    # The message bounced back as expected.
    if message != "fw data with a \0 byte"
      puts "The returned message data did not match!"
      puts "  Expected: 'fw data with a \\x00 byte'"
      puts "  Actual:  #{message.inspect}"
      exit(1)
    end
    puts "Received message: #{message.inspect}"

    if self.messagesReceived == TOTAL_TASKS
      if self.messagesReceived != self.messagesSent
        puts "Sent #{self.messagesSent}"
        puts "but received #{self.messagesReceived}"
        exit(1)
      end
      puts "All tasks done, and all messages received, exiting"
      driver.stop()
    end
  end
end

if __FILE__ == $0
  raise if ARGV.size == 0

  executor = Mesos::ExecutorInfo.new
  executor.executor_id.value = "default"
  executor.command.value = "/vagrant/ruby-drivers/examples/test_executor.rb"
  executor.name = "Test Executor (Ruby)"
  executor.source = "ruby_test"

  framework = Mesos::FrameworkInfo.new
  framework.id = Mesos::FrameworkID.new(value: "example")
  framework.user = "" # Have Mesos fill in the current user.
  framework.name = "Test Framework (Ruby)"

  driver = Mesos::MesosSchedulerDriver.new(
    TestScheduler.new(executor),
    framework,
    ARGV[0])

  if driver.run == Mesos::Status::DRIVER_STOPPED
    exit 0
  else
    exit 1
  end
end
