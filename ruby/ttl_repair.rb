# frozen_string_literal: true

require 'io/console'
require 'redis'
require 'redis-clustering'
require 'benchmark'

BATCH_SIZE = 100
LATENCY_BUFFER = 100

p 'source redis url (includes rediss:// and port):'
source_redis_endpoint = $stdin.noecho(&:gets).chomp

p 'target memorydb url:'
target_memorydb_endpoint = $stdin.noecho(&:gets).chomp

p 'dryrun? (y/n)'
dryrun = gets.chomp == 'y'

source_elasticache = Redis.new(url: source_redis_endpoint)
target_memdb = Redis::Cluster.new(nodes: [target_memorydb_endpoint], reconnect_attempts: 3, timeout: 0.1,
                                  slow_command_timeout: 0.1)

cursor = 0
updated_keys = []
skipped_keys = []
scanned_keys = 0

p "#{Time.now} START"

p(Benchmark.measure do
  loop do
    p "#{Time.now} cursor: #{cursor}"
    cursor, keys = target_memdb.scan(cursor, count: BATCH_SIZE, type: 'string')

    # bulk get TTL of keys from memdb
    memdb_exp_values = target_memdb.pipelined do |mem_pipelined|
      keys.each do |k|
        if mem_pipelined.respond_to?(:pexpiretime)
          mem_pipelined.pexpiretime(k)
        else
          # add to skipped
          skipped_keys << "memdb:#{k}"
          -1
        end
      end
    end

    # bulk get TTL / expiretime of keys from elasticache
    e_exp_values = source_elasticache.pipelined do |ec_pipelined|
      keys.each do |k|
        # we assume that if mem db doesnt have expires, ec wont have it either
        if ec_pipelined.respond_to?(:pexpiretime)
          ec_pipelined.pexpiretime(k)
        else
          skipped_keys << "ec:#{k}"
          -1
        end
      end
    end

    # bulk update expiretime for any differences
    target_memdb.pipelined do |pipelined|
      memdb_exp_values.each_with_index do |memdb_val, i|
        e_val = e_exp_values[i]

        next if e_val == -1

        p "e_exp: #{e_val} memdb_exp: #{memdb_val}"
        next unless (memdb_val - e_val).abs > LATENCY_BUFFER

        p 'exp values do not match!'
        pipelined.pexpireat(keys[i], e_val) unless dryrun

        updated_keys << { key: keys[i], old_value: memdb_val, new_value: e_val }
      end
    end

    scanned_keys += keys.size
    p "#{Time.now} now scanned: #{scanned_keys}!\n"

    break if cursor == '0'
  end
end)

p ''
p "#{Time.now} FINISHED"
p "number of keys scanned: #{scanned_keys}"
p "number of key TTLs updated if not dryrun: #{updated_keys.size}"
p "skipped keys: #{skipped_keys}"
p updated_keys
