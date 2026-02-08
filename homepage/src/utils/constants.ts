/**
 * Application constants for the MirDB Homepage.
 */

export const GITHUB_URL = 'https://github.com/yetone/mirdb';
export const DOCUMENTATION_URL = 'https://github.com/yetone/mirdb#readme';
export const PRODUCT_NAME = 'MirDB';
export const TAGLINE = 'A Persistent Key-Value Store with Memcached Protocol';

export const FEATURES = [
  {
    id: 'memcached',
    title: 'Memcached Protocol',
    description: 'Compatible with standard memcached text protocol for seamless integration',
  },
  {
    id: 'persistence',
    title: 'Persistence',
    description: 'Data persisted to disk using SSTables for durability',
  },
  {
    id: 'lsm',
    title: 'LSM Tree Architecture',
    description: 'Log-Structured Merge-tree for optimal write performance',
  },
];

export const INSTALLATION_COMMANDS = [
  'git clone https://github.com/yetone/mirdb.git',
  'cd mirdb',
  'cargo build --release',
];
