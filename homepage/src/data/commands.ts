/**
 * Memcached commands reference for the MirDB homepage
 * Owner: Scenario 5 - Usage Demonstration
 */

import type { Command } from '@/types'

export const commands: Command[] = [
  {
    name: 'get',
    syntax: 'get <key1> <key2> ...',
    description: 'Retrieve values by key. Supports multiple keys in a single request.',
  },
  {
    name: 'gets',
    syntax: 'gets <key1> <key2> ...',
    description: 'Retrieve values with CAS (Check-And-Set) support for optimistic locking.',
  },
  {
    name: 'set',
    syntax: 'set <key> <flags> <ttl> <bytes>',
    description: 'Set a key-value pair. Creates or updates the entry.',
  },
  {
    name: 'add',
    syntax: 'add <key> <flags> <ttl> <bytes>',
    description: 'Add a key-value pair only if the key does not already exist.',
  },
  {
    name: 'replace',
    syntax: 'replace <key> <flags> <ttl> <bytes>',
    description: 'Replace a key-value pair only if the key already exists.',
  },
  {
    name: 'append',
    syntax: 'append <key> <flags> <ttl> <bytes>',
    description: 'Append data to an existing value without affecting flags or TTL.',
  },
  {
    name: 'prepend',
    syntax: 'prepend <key> <flags> <ttl> <bytes>',
    description: 'Prepend data to an existing value without affecting flags or TTL.',
  },
  {
    name: 'delete',
    syntax: 'delete <key>',
    description: 'Delete a key from the database.',
  },
  {
    name: 'info',
    syntax: 'info',
    description: 'Query database information and server statistics.',
  },
  {
    name: 'major_compaction',
    syntax: 'major_compaction',
    description: 'Trigger a full compaction of the LSM-Tree storage engine.',
  },
]

export const defaultConfig = `addr = "0.0.0.0:12333"
max_level = 7
work_dir = "/tmp/mirdb"
sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32
imm_mem_table_max_count = 16
block_size = "4K"
block_restart_interval = 16
l0_compaction_trigger = 4
thread_sleep_ms = 500`
