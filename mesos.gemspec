# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'mesos/version'

Gem::Specification.new do |gem|
  gem.name          = "mesos"
  gem.version       = Mesos::VERSION
  gem.authors       = ["Burke Libbey"]
  gem.email         = ["burke.libbey@shopify.com"]
  gem.description   = %q{Drivers to write Mesos Frameworks in Ruby}
  gem.summary       = %q{Drivers to write Mesos Frameworks in Ruby}
  gem.homepage      = ""

  gem.extensions    = ['ext/mesos_ext/extconf.rb']
  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_runtime_dependency "ruby-protocol-buffers"
end
