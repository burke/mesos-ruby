
$:.unshift(File.expand_path "../lib", __FILE__)
$:.unshift(File.expand_path "../ext/mesos_ext", __FILE__)

system "cd ext/mesos_ext ; make"

task :irb do
  require 'mesos'
  require 'pry'
  Pry.start
end

task :test do
  Dir.glob("./test/**_test.rb").each { |f| require f }
  exit
end
