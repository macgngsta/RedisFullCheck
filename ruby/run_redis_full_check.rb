# frozen_string_literal: true

require "io/console"
require "uri"

p "source redis url:"
source_redis_url = STDIN.noecho(&:gets).chomp

source_uri = URI.parse(source_redis_url)
source_redis_endpoint = "#{source_uri.host}:#{source_uri.port}"
source_redis_pwd = source_uri.password

p "target memorydb url:"
target_memorydb_url = STDIN.noecho(&:gets).chomp
target_uri = URI.parse(target_memorydb_url)
target_memorydb_endpoint = "#{target_uri.host}:#{target_uri.port}"
target_memorydb_auth = "#{target_uri.user}:#{target_uri.password}"

# call RedisFullCheck
cmd = `../bin/redis-full-check -s #{source_redis_endpoint} -p #{source_redis_pwd} -t #{target_memorydb_endpoint} -a #{target_memorydb_auth} --targetdbtype=1 --comparemode=1 --comparetimes=3`

puts cmd