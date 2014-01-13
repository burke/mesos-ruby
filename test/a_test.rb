require 'minitest/autorun'

require 'mesos'

class FooTest < MiniTest::Unit::TestCase
  def test_blah
    assert_equal 42, Mesos::MesosExecutorDriver.new(42).executor
  end
end
