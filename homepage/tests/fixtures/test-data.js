/**
 * Test Fixtures and Data
 * Owner: Scenario 1 - Hero Section Display
 *
 * Shared test data for all scenarios.
 */

// Hero Section Test Data
export const HERO_DATA = {
  title: 'MiRDB',
  tagline: 'A persistent key-value store with full Memcached protocol support',
  keywords: ['persistent', 'key-value store', 'memcached protocol'],
  githubUrl: 'https://github.com/yetone/mirdb',
  quickstartAnchor: '#quickstart',
};

// Quick Start Section Test Data (Scenario 3)
export const QUICKSTART_STEPS = [
  {
    number: 1,
    title: 'Download Pre-built Binary',
    description: 'Download the latest pre-built binary from the GitHub Releases page',
  },
  {
    number: 2,
    title: 'Or Install via Cargo',
    command: 'cargo install mirdb',
  },
  {
    number: 3,
    title: 'Run the Server',
    command: 'mirdb-server',
  },
  {
    number: 4,
    title: 'Connect with a Client',
    command: 'telnet localhost 12333',
  },
];

export const EXPECTED_COMMANDS = {
  cargoInstall: 'cargo install mirdb',
  runServer: 'mirdb-server',
  connect: 'telnet localhost 12333',
};

// Features Data - Scenario 2 will extend
export const FEATURES = [];

// Configuration Values - Scenario 6 will extend
export const CONFIG_VALUES = [];

// Project Status Items - Scenario 14 will extend
export const STATUS_ITEMS = [];

// Code Examples - Scenario 4 will extend
export const CODE_EXAMPLES = [];

// Export all for convenience
export default {
  HERO_DATA,
  QUICKSTART_STEPS,
  EXPECTED_COMMANDS,
  FEATURES,
  CONFIG_VALUES,
  STATUS_ITEMS,
  CODE_EXAMPLES,
};
