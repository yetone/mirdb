/**
 * Application constants.
 */
import type { Feature, RoadmapItem, CodeExample } from '$lib/types';

export const GITHUB_URL = 'https://github.com/yetone/mirdb';

export const FEATURES: Feature[] = [
	{
		icon: '🔌',
		title: 'Memcached Compatible',
		description: 'Drop-in replacement for memcached with full protocol support. Use your existing memcached clients without any code changes.'
	},
	{
		icon: '💾',
		title: 'Persistent Storage',
		description: 'Unlike memcached, MirDB persists data to disk using SSTables. Your data survives restarts and crashes.'
	},
	{
		icon: '🌳',
		title: 'LSM-Tree Architecture',
		description: 'Built on a Log-Structured Merge-tree for optimal write performance with efficient reads through leveled compaction.'
	},
	{
		icon: '🦀',
		title: 'Rust Implementation',
		description: 'Written in Rust for memory safety, zero-cost abstractions, and fearless concurrency without garbage collection.'
	},
	{
		icon: '📝',
		title: 'Write-Ahead Logging',
		description: 'WAL ensures durability by writing operations to disk before acknowledging. Recover cleanly from any failure.'
	},
	{
		icon: '🔄',
		title: 'Compaction Support',
		description: 'Both minor and major compaction strategies keep storage efficient and read performance optimal over time.'
	}
];

export const ROADMAP_ITEMS: RoadmapItem[] = [
	{ title: 'Tokio with Memcached protocol', completed: true },
	{ title: 'Memtable with skiplist', completed: true },
	{ title: 'Minor compaction', completed: true },
	{ title: 'Major compaction', completed: true },
	{ title: 'Raft consensus', completed: false }
];

export const INSTALLATION_CODE: CodeExample[] = [
	{
		language: 'bash',
		code: 'cargo install mirdb',
		description: 'Install MirDB'
	},
	{
		language: 'bash',
		code: 'mirdb-server',
		description: 'Start the server'
	},
	{
		language: 'bash',
		code: `echo "set mykey 0 0 5\\r\\nvalue\\r\\n" | nc localhost 12333`,
		description: 'Set a value'
	}
];

export const ASCII_LOGO = `
 __  __ _      ____  ____
|  \\/  (_)_ __|  _ \\| __ )
| |\\/| | | '__| | | |  _ \\
| |  | | | |  | |_| | |_) |
|_|  |_|_|_|  |____/|____/
`;
