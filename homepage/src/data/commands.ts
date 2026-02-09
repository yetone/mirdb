/**
 * Command data for the MirDB Homepage.
 * Owner: Scenario 5 - Protocol Documentation
 *
 * Contains all supported Memcached commands and MirDB-specific commands.
 */

import type { Command } from '../types';

export interface ProtocolCommand extends Command {
  isMirDBSpecific?: boolean;
}

export const commands: ProtocolCommand[] = [
  {
    name: 'SET',
    syntax: 'set <key> <flags> <ttl> <bytes> [noreply]\\r\\n<data>\\r\\n',
    description: 'Store a key-value pair. If the key already exists, it will be overwritten.',
    example: 'set mykey 0 3600 5\\r\\nhello\\r\\n',
  },
  {
    name: 'GET',
    syntax: 'get <key1> [<key2> ...]\\r\\n',
    description: 'Retrieve one or more keys. Returns VALUE blocks for each found key.',
    example: 'get mykey\\r\\n',
  },
  {
    name: 'DELETE',
    syntax: 'delete <key> [noreply]\\r\\n',
    description: 'Remove a key from the store. Returns DELETED if successful, NOT_FOUND if the key does not exist.',
    example: 'delete mykey\\r\\n',
  },
  {
    name: 'ADD',
    syntax: 'add <key> <flags> <ttl> <bytes> [noreply]\\r\\n<data>\\r\\n',
    description: 'Store a key-value pair only if the key does not already exist. Returns NOT_STORED if key exists.',
    example: 'add newkey 0 3600 5\\r\\nvalue\\r\\n',
  },
  {
    name: 'REPLACE',
    syntax: 'replace <key> <flags> <ttl> <bytes> [noreply]\\r\\n<data>\\r\\n',
    description: 'Store a key-value pair only if the key already exists. Returns NOT_STORED if key does not exist.',
    example: 'replace mykey 0 3600 8\\r\\nnewvalue\\r\\n',
  },
  {
    name: 'APPEND',
    syntax: 'append <key> <flags> <ttl> <bytes> [noreply]\\r\\n<data>\\r\\n',
    description: 'Append data to an existing key\'s value. The key must already exist.',
    example: 'append mykey 0 0 6\\r\\n_added\\r\\n',
  },
  {
    name: 'PREPEND',
    syntax: 'prepend <key> <flags> <ttl> <bytes> [noreply]\\r\\n<data>\\r\\n',
    description: 'Prepend data to an existing key\'s value. The key must already exist.',
    example: 'prepend mykey 0 0 7\\r\\npre_\\r\\n',
  },
  {
    name: 'INFO',
    syntax: 'info\\r\\n',
    description: 'Display database status and level information. Returns statistics about the storage engine.',
    example: 'info\\r\\n',
    isMirDBSpecific: true,
  },
  {
    name: 'MAJOR_COMPACTION',
    syntax: 'major_compaction\\r\\n',
    description: 'Trigger a manual major compaction to merge all SSTables and reclaim disk space.',
    example: 'major_compaction\\r\\n',
    isMirDBSpecific: true,
  },
];
