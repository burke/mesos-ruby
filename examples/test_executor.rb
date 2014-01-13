#!/usr/bin/env ruby

require 'rubygems'
require 'protocol_buffers'
require 'mesos'

class MyExecutor < Mesos::Executor

  def announce(s, args)
    $stderr << "\x1b[31m#{s} {\x1b[0m\n"
    args.each.with_index { |a,i| $stderr << "  \x1b[31m#{i}.\x1b[0m " + a.inspect + "\n"}
    $stderr << "\x1b[31m}\x1b[0m\n"
  end

  def trace(s)
    $stderr << "\x1b[34m#{s}\x1b[0m\n"
  end

  %w(registered reregistered disconnected kill_task error shutdown).each do |m|
    class_eval <<-CODE
      def #{m}(*args)
        announce #{m.to_s.upcase.inspect}, args
      end
    CODE
  end

  def launch_task(driver, task)
    announce "LAUNCH_TASK", [driver, task]
    # Create a thread to run the task. Tasks should always be run in new
    # threads or processes, rather than inside launchTask itself.
    Thread.new { run_task(driver, task) }
  end

  def run_task(driver, task)
    announce "RUN_TASK", [driver, task]
    update = Mesos::TaskStatus.new
    update.task_id.value = task.task_id.value
    update.state = Mesos::TaskState::TASK_RUNNING
    update.data = "data with a \0 byte"
    trace "Sending status update 1"
    driver.send_status_update(update)
    trace "Sent status update 1"

    sleep 1

    update = Mesos::TaskStatus.new
    update.task_id.value = task.task_id.value
    update.state = Mesos::TaskState::TASK_FINISHED
    update.data = "data with a \0 byte"
    trace "Sending status update 2"
    puts driver.send_status_update(update)
    trace "Sent status update 2"
  end

  def framework_message(driver, message)
    announce "FRAMEWORK_MESSAGE", [driver, message]
    # Send it back to the scheduler.
    driver.send_framework_message(message)
  end
end

if __FILE__ == $0
  puts "Starting executor"
  driver = Mesos::MesosExecutorDriver.new(MyExecutor.new)
  if driver.run == Mesos::Status::DRIVER_STOPPED
    exit 0
  else
    exit 1
  end
end
