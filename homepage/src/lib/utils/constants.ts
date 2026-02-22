/**
 * Application constants.
 */

import type { Feature, RoadmapItem, CodeExample } from '$lib/types/index.js';

export const GITHUB_URL = 'https://github.com/yetone/mirdb';

export const ASCII_LOGO = `
███╗   ███╗██╗██████╗ ██████╗ ██████╗
████╗ ████║██║██╔══██╗██╔══██╗██╔══██╗
██╔████╔██║██║██████╔╝██║  ██║██████╔╝
██║╚██╔╝██║██║██╔══██╗██║  ██║██╔══██╗
██║ ╚═╝ ██║██║██║  ██║██████╔╝██████╔╝
╚═╝     ╚═╝╚═╝╚═╝  ╚═╝╚═════╝ ╚═════╝
`.trim();

export const TAGLINE = 'A Persistent Key-Value Store with Memcached protocol';

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
		icon: '🌲',
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
		icon: '🗜️',
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
		code: 'git clone https://github.com/yetone/mirdb.git\ncd mirdb\ncargo build --release',
		description: 'Clone and build MirDB'
	},
	{
		language: 'bash',
		code: './target/release/mirdb',
		description: 'Run MirDB server'
	},
	{
		language: 'bash',
		code: 'telnet localhost 12333\nset foo 0 0 3\nbar\nget foo',
		description: 'Connect and test'
	}
];
