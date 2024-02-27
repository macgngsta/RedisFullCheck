# frozen_string_literal: true

# value repair script
require "io/console"

p "source redis - endpoint:"
source_redis_endpoint = STDIN.noecho(&:gets).chomp

p "source redis - pwd"
source_redis_pwd = STDIN.noecho(&:gets).chomp

p "target memorydb endpoint:"
target_memorydb_endpoint = STDIN.noecho(&:gets).chomp

p "target memorydb - auth"
target_memorydb_auth = STDIN.noecho(&:gets).chomp

# call RedisFullCheck
cmd = `../bin/redis-full-check -s #{source_redis_endpoint} -p #{source_redis_pwd} -t #{target_memorydb_endpoint} -a #{target_memorydb_auth} --targetdbtype=1 --comparemode=1 --comparetimes=3`

puts cmd
