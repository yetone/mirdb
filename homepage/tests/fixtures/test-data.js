/**
 * Test Fixtures and Data
 * Owner: Scenario 1 - Hero Section Display
 */

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
