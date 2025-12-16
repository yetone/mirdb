/**
 * Content Accuracy Tests
 * Scenario: Verify that all technical content matches MirDB's actual capabilities
 *
 * This test suite verifies:
 * 1. Documented commands match PRD Appendix B
 * 2. Configuration defaults match PRD Appendix C
 * 3. Raft consensus is marked as planned (not implemented)
 */

const fs = require('fs');
const path = require('path');

describe('Content Accuracy', () => {
  let document;
  let htmlContent;

  // PRD Appendix B - Supported Commands Reference
  const PRD_COMMANDS = [
    'SET',
    'GET',
    'DELETE',
    'ADD',
    'REPLACE',
    'APPEND',
    'PREPEND',
    'INFO',
    'MAJOR_COMPACTION'
  ];

  // PRD Appendix C - Default Configuration Reference
  const PRD_CONFIG_DEFAULTS = {
    'addr': '0.0.0.0:12333',
    'max_level': '7',
    'work_dir': '/tmp/mirdb',
    'sst_max_size': '100M',
    'mem_table_max_size': '4M',
    'block_size': '4K',
    'l0_compaction_trigger': '4',
    'thread_sleep_ms': '500'
  };

  beforeAll(() => {
    // Load the index.html file
    const htmlPath = path.resolve(__dirname, '../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  // Test Case 1: Compare documented commands to PRD Appendix B
  describe('Test Case 1: Command Accuracy (PRD Appendix B)', () => {
    test('All PRD commands are documented in the homepage', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      expect(commandsSection).not.toBeNull();

      const commandCards = commandsSection.querySelectorAll('.command-card');
      const documentedCommands = [];

      for (const card of commandCards) {
        const heading = card.querySelector('h3');
        if (heading) {
          documentedCommands.push(heading.textContent.toUpperCase().trim());
        }
      }

      // Verify all PRD commands are documented
      for (const prdCommand of PRD_COMMANDS) {
        expect(documentedCommands).toContain(prdCommand);
      }
    });

    test('SET command is documented with correct syntax', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      const commandCards = commandsSection.querySelectorAll('.command-card');

      let setCommand = null;
      for (const card of commandCards) {
        const heading = card.querySelector('h3');
        if (heading && heading.textContent.toUpperCase().trim() === 'SET') {
          setCommand = card;
          break;
        }
      }

      expect(setCommand).not.toBeNull();

      const description = setCommand.querySelector('.command-description');
      expect(description).not.toBeNull();
      expect(description.textContent.toLowerCase()).toContain('store');
    });

    test('GET command is documented with correct syntax', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      const commandCards = commandsSection.querySelectorAll('.command-card');

      let getCommand = null;
      for (const card of commandCards) {
        const heading = card.querySelector('h3');
        if (heading && heading.textContent.toUpperCase().trim() === 'GET') {
          getCommand = card;
          break;
        }
      }

      expect(getCommand).not.toBeNull();

      const description = getCommand.querySelector('.command-description');
      expect(description).not.toBeNull();
      expect(description.textContent.toLowerCase()).toContain('retrieve');
    });

    test('DELETE command is documented correctly', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      const commandCards = commandsSection.querySelectorAll('.command-card');

      let deleteCommand = null;
      for (const card of commandCards) {
        const heading = card.querySelector('h3');
        if (heading && heading.textContent.toUpperCase().trim() === 'DELETE') {
          deleteCommand = card;
          break;
        }
      }

      expect(deleteCommand).not.toBeNull();

      const description = deleteCommand.querySelector('.command-description');
      expect(description).not.toBeNull();
      expect(description.textContent.toLowerCase()).toContain('remove');
    });

    test('ADD command is documented correctly', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      const commandCards = commandsSection.querySelectorAll('.command-card');

      let addCommand = null;
      for (const card of commandCards) {
        const heading = card.querySelector('h3');
        if (heading && heading.textContent.toUpperCase().trim() === 'ADD') {
          addCommand = card;
          break;
        }
      }

      expect(addCommand).not.toBeNull();

      const description = addCommand.querySelector('.command-description');
      expect(description).not.toBeNull();
      // ADD stores only if key doesn't exist
      expect(description.textContent.toLowerCase()).toMatch(/doesn't exist|does not exist|not exist/);
    });

    test('REPLACE command is documented correctly', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      const commandCards = commandsSection.querySelectorAll('.command-card');

      let replaceCommand = null;
      for (const card of commandCards) {
        const heading = card.querySelector('h3');
        if (heading && heading.textContent.toUpperCase().trim() === 'REPLACE') {
          replaceCommand = card;
          break;
        }
      }

      expect(replaceCommand).not.toBeNull();

      const description = replaceCommand.querySelector('.command-description');
      expect(description).not.toBeNull();
      // REPLACE stores only if key exists
      expect(description.textContent.toLowerCase()).toContain('exists');
    });

    test('APPEND command is documented correctly', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      const commandCards = commandsSection.querySelectorAll('.command-card');

      let appendCommand = null;
      for (const card of commandCards) {
        const heading = card.querySelector('h3');
        if (heading && heading.textContent.toUpperCase().trim() === 'APPEND') {
          appendCommand = card;
          break;
        }
      }

      expect(appendCommand).not.toBeNull();

      const description = appendCommand.querySelector('.command-description');
      expect(description).not.toBeNull();
      expect(description.textContent.toLowerCase()).toContain('append');
    });

    test('PREPEND command is documented correctly', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      const commandCards = commandsSection.querySelectorAll('.command-card');

      let prependCommand = null;
      for (const card of commandCards) {
        const heading = card.querySelector('h3');
        if (heading && heading.textContent.toUpperCase().trim() === 'PREPEND') {
          prependCommand = card;
          break;
        }
      }

      expect(prependCommand).not.toBeNull();

      const description = prependCommand.querySelector('.command-description');
      expect(description).not.toBeNull();
      expect(description.textContent.toLowerCase()).toContain('prepend');
    });

    test('INFO command is documented correctly', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      const commandCards = commandsSection.querySelectorAll('.command-card');

      let infoCommand = null;
      for (const card of commandCards) {
        const heading = card.querySelector('h3');
        if (heading && heading.textContent.toUpperCase().trim() === 'INFO') {
          infoCommand = card;
          break;
        }
      }

      expect(infoCommand).not.toBeNull();

      const description = infoCommand.querySelector('.command-description');
      expect(description).not.toBeNull();
      expect(description.textContent.toLowerCase()).toMatch(/status|database|level/);
    });

    test('MAJOR_COMPACTION command is documented correctly', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      const commandCards = commandsSection.querySelectorAll('.command-card');

      let majorCompactionCommand = null;
      for (const card of commandCards) {
        const heading = card.querySelector('h3');
        if (heading && heading.textContent.toUpperCase().trim() === 'MAJOR_COMPACTION') {
          majorCompactionCommand = card;
          break;
        }
      }

      expect(majorCompactionCommand).not.toBeNull();

      const description = majorCompactionCommand.querySelector('.command-description');
      expect(description).not.toBeNull();
      expect(description.textContent.toLowerCase()).toContain('compaction');
    });

    test('No undocumented commands are claimed (accuracy check)', () => {
      const commandsSection = document.querySelector('#commands, .commands');
      const commandCards = commandsSection.querySelectorAll('.command-card');
      const documentedCommands = [];

      for (const card of commandCards) {
        const heading = card.querySelector('h3');
        if (heading) {
          documentedCommands.push(heading.textContent.toUpperCase().trim());
        }
      }

      // Verify documented commands are all valid PRD commands
      // (No false claims about commands that don't exist)
      for (const docCommand of documentedCommands) {
        expect(PRD_COMMANDS).toContain(docCommand);
      }
    });
  });

  // Test Case 2: Compare configuration defaults to PRD Appendix C
  describe('Test Case 2: Configuration Accuracy (PRD Appendix C)', () => {
    test('addr default matches PRD (0.0.0.0:12333)', () => {
      const configSection = document.querySelector('#configuration, .configuration');
      expect(configSection).not.toBeNull();

      const sectionContent = configSection.textContent;
      expect(sectionContent).toContain(PRD_CONFIG_DEFAULTS.addr);
    });

    test('work_dir default matches PRD (/tmp/mirdb)', () => {
      const configSection = document.querySelector('#configuration, .configuration');
      expect(configSection).not.toBeNull();

      const sectionContent = configSection.textContent;
      expect(sectionContent).toContain(PRD_CONFIG_DEFAULTS.work_dir);
    });

    test('max_level default matches PRD (7)', () => {
      const configSection = document.querySelector('#configuration, .configuration');
      expect(configSection).not.toBeNull();

      const configTable = configSection.querySelector('.config-table, table');
      expect(configTable).not.toBeNull();

      // Find the row for max_level
      const rows = configTable.querySelectorAll('tr');
      let foundMaxLevel = false;

      for (const row of rows) {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 1 && cells[0].textContent.includes('max_level')) {
          foundMaxLevel = true;
          // Default should be 7
          expect(row.textContent).toContain('7');
          break;
        }
      }

      expect(foundMaxLevel).toBe(true);
    });

    test('sst_max_size default matches PRD (100M)', () => {
      const configSection = document.querySelector('#configuration, .configuration');
      expect(configSection).not.toBeNull();

      const sectionContent = configSection.textContent;
      expect(sectionContent).toMatch(/sst_max_size/);
      expect(sectionContent).toMatch(/100M|100MB/);
    });

    test('mem_table_max_size default matches PRD (4M)', () => {
      const configSection = document.querySelector('#configuration, .configuration');
      expect(configSection).not.toBeNull();

      const sectionContent = configSection.textContent;
      expect(sectionContent).toMatch(/mem_table_max_size/);
      expect(sectionContent).toMatch(/4M|4MB/);
    });

    test('block_size default matches PRD (4K)', () => {
      const configSection = document.querySelector('#configuration, .configuration');
      expect(configSection).not.toBeNull();

      const sectionContent = configSection.textContent;
      expect(sectionContent).toMatch(/block_size/);
      expect(sectionContent).toMatch(/4K|4KB/);
    });

    test('l0_compaction_trigger default matches PRD (4)', () => {
      const configSection = document.querySelector('#configuration, .configuration');
      expect(configSection).not.toBeNull();

      const configTable = configSection.querySelector('.config-table, table');
      expect(configTable).not.toBeNull();

      const rows = configTable.querySelectorAll('tr');
      let foundL0Trigger = false;

      for (const row of rows) {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 1 && cells[0].textContent.includes('l0_compaction_trigger')) {
          foundL0Trigger = true;
          // Default should be 4
          expect(row.textContent).toContain('4');
          break;
        }
      }

      expect(foundL0Trigger).toBe(true);
    });

    test('thread_sleep_ms default matches PRD (500)', () => {
      const configSection = document.querySelector('#configuration, .configuration');
      expect(configSection).not.toBeNull();

      const configTable = configSection.querySelector('.config-table, table');
      expect(configTable).not.toBeNull();

      const rows = configTable.querySelectorAll('tr');
      let foundThreadSleep = false;

      for (const row of rows) {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 1 && cells[0].textContent.includes('thread_sleep_ms')) {
          foundThreadSleep = true;
          // Default should be 500
          expect(row.textContent).toContain('500');
          break;
        }
      }

      expect(foundThreadSleep).toBe(true);
    });

    test('Configuration example TOML is accurate', () => {
      const configSection = document.querySelector('#configuration, .configuration');
      expect(configSection).not.toBeNull();

      const codeBlocks = configSection.querySelectorAll('pre code, pre');
      let tomlContent = '';

      for (const block of codeBlocks) {
        tomlContent += block.textContent;
      }

      // Verify TOML includes accurate default values
      expect(tomlContent).toMatch(/addr\s*=\s*["']0\.0\.0\.0:12333["']/);
      expect(tomlContent).toMatch(/work_dir\s*=\s*["']\/tmp\/mirdb["']/);
      expect(tomlContent).toMatch(/max_level\s*=\s*7/);
      expect(tomlContent).toMatch(/sst_max_size\s*=\s*["']100M["']/);
      expect(tomlContent).toMatch(/mem_table_max_size\s*=\s*["']4M["']/);
      expect(tomlContent).toMatch(/block_size\s*=\s*["']4K["']/);
      expect(tomlContent).toMatch(/l0_compaction_trigger\s*=\s*4/);
    });
  });

  // Test Case 3: Verify Raft consensus is marked as planned (not implemented)
  describe('Test Case 3: Raft Consensus Status Accuracy', () => {
    test('Raft consensus feature is mentioned on the page', () => {
      const pageContent = document.body.textContent.toLowerCase();
      expect(pageContent).toMatch(/raft/i);
    });

    test('Raft/distributed feature is marked as planned (not implemented)', () => {
      const statusSection = document.querySelector('#status, .project-status, .status');
      expect(statusSection).not.toBeNull();

      // Look for planned section
      const plannedItems = statusSection.querySelectorAll('.status-item.planned, [data-status="planned"], .planned');

      let raftIsPlanned = false;
      for (const item of plannedItems) {
        const itemText = item.textContent.toLowerCase();
        if (itemText.includes('raft') || itemText.includes('distributed')) {
          raftIsPlanned = true;
          break;
        }
      }

      expect(raftIsPlanned).toBe(true);
    });

    test('Raft is NOT claimed as implemented', () => {
      const statusSection = document.querySelector('#status, .project-status, .status');
      expect(statusSection).not.toBeNull();

      // Look for implemented section
      const implementedItems = statusSection.querySelectorAll('.status-item.implemented, [data-status="implemented"], .implemented');

      let raftIsImplemented = false;
      for (const item of implementedItems) {
        const itemText = item.textContent.toLowerCase();
        if (itemText.includes('raft')) {
          raftIsImplemented = true;
          break;
        }
      }

      // Raft should NOT be in the implemented list
      expect(raftIsImplemented).toBe(false);
    });

    test('Planned status badge exists for Raft feature', () => {
      const statusSection = document.querySelector('#status, .project-status, .status');
      expect(statusSection).not.toBeNull();

      // Find the Raft feature item
      const statusItems = statusSection.querySelectorAll('.status-item, li');
      let raftItem = null;

      for (const item of statusItems) {
        if (item.textContent.toLowerCase().includes('raft')) {
          raftItem = item;
          break;
        }
      }

      expect(raftItem).not.toBeNull();

      // Check for planned badge
      const badge = raftItem.querySelector('.status-badge, .badge');
      expect(badge).not.toBeNull();
      expect(badge.textContent.toLowerCase()).toContain('planned');
    });

    test('Implemented features do not overpromise (only actual capabilities)', () => {
      const statusSection = document.querySelector('#status, .project-status, .status');
      expect(statusSection).not.toBeNull();

      const implementedColumn = statusSection.querySelector('.status-column:first-child, .implemented-column');
      expect(implementedColumn).not.toBeNull();

      const implementedText = implementedColumn.textContent.toLowerCase();

      // Verify only actual implemented features are listed
      // These features ARE implemented per the knowledge base:
      expect(implementedText).toMatch(/memcached|protocol/);
      expect(implementedText).toMatch(/persistent|storage/);
      expect(implementedText).toMatch(/lsm|tree/);
      expect(implementedText).toMatch(/minor.*compaction|compaction/);
      expect(implementedText).toMatch(/major.*compaction|compaction/);
      expect(implementedText).toMatch(/wal|write-ahead/);

      // Verify Raft is NOT in implemented
      expect(implementedText).not.toMatch(/\braft\b/);
    });
  });
});
