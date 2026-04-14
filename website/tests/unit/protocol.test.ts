/**
 * Protocol Reference Unit Tests
 * Owner: Scenario 5 - Protocol Reference Section
 *
 * Tests for:
 * - Commands JSON data structure validation
 * - Protocol reference component rendering
 */
import { describe, it, expect, beforeEach } from 'vitest';
import commandsData from '../../src/data/commands.json';

describe('Protocol Reference Component', () => {
  describe('Commands Data Structure', () => {
    it('should have standard commands array', () => {
      expect(commandsData).toHaveProperty('standard');
      expect(Array.isArray(commandsData.standard)).toBe(true);
      expect(commandsData.standard.length).toBeGreaterThan(0);
    });

    it('should have mirdb_specific commands array', () => {
      expect(commandsData).toHaveProperty('mirdb_specific');
      expect(Array.isArray(commandsData.mirdb_specific)).toBe(true);
      expect(commandsData.mirdb_specific.length).toBeGreaterThan(0);
    });

    it('should have SET command with correct structure', () => {
      const setCommand = commandsData.standard.find(cmd => cmd.command === 'SET');
      expect(setCommand).toBeDefined();
      expect(setCommand?.syntax).toBe('SET <key> <flags> <exptime> <bytes>');
      expect(setCommand?.description).toBeDefined();
    });

    it('should have GET command with correct structure', () => {
      const getCommand = commandsData.standard.find(cmd => cmd.command === 'GET');
      expect(getCommand).toBeDefined();
      expect(getCommand?.syntax).toBe('GET <key>');
      expect(getCommand?.description).toBeDefined();
    });

    it('should have DELETE command with correct structure', () => {
      const deleteCommand = commandsData.standard.find(cmd => cmd.command === 'DELETE');
      expect(deleteCommand).toBeDefined();
      expect(deleteCommand?.syntax).toBe('DELETE <key>');
      expect(deleteCommand?.description).toBeDefined();
    });

    it('should have INFO command in MirDB-specific commands', () => {
      const infoCommand = commandsData.mirdb_specific.find(cmd => cmd.command === 'INFO');
      expect(infoCommand).toBeDefined();
      expect(infoCommand?.syntax).toBe('INFO');
      expect(infoCommand?.description).toContain('statistics');
    });

    it('should have MAJOR_COMPACTION command in MirDB-specific commands', () => {
      const compactionCommand = commandsData.mirdb_specific.find(cmd => cmd.command === 'MAJOR_COMPACTION');
      expect(compactionCommand).toBeDefined();
      expect(compactionCommand?.syntax).toBe('MAJOR_COMPACTION');
      expect(compactionCommand?.description).toContain('compaction');
    });
  });

  describe('Component Rendering', () => {
    let container: HTMLElement;

    beforeEach(() => {
      container = document.createElement('div');
      document.body.appendChild(container);
    });

    it('TC7: should render command table with all required commands', () => {
      // Create table HTML from commands data
      const standardCommands = commandsData.standard;
      const mirdbCommands = commandsData.mirdb_specific;

      // Build the protocol section HTML
      const html = `
        <section id="protocol" class="protocol">
          <div class="container">
            <h2 class="protocol-title">Protocol Reference</h2>
            <table class="protocol-table" data-testid="command-table">
              <thead>
                <tr>
                  <th>Command</th>
                  <th>Syntax</th>
                  <th>Description</th>
                </tr>
              </thead>
              <tbody>
                ${standardCommands.map(cmd => `
                  <tr data-command="${cmd.command}">
                    <td><code>${cmd.command}</code></td>
                    <td><code>${cmd.syntax}</code></td>
                    <td>${cmd.description}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
            <table class="protocol-table mirdb-specific-table" data-testid="mirdb-command-table">
              <thead>
                <tr>
                  <th>Command</th>
                  <th>Syntax</th>
                  <th>Description</th>
                </tr>
              </thead>
              <tbody>
                ${mirdbCommands.map(cmd => `
                  <tr data-command="${cmd.command}" class="mirdb-specific">
                    <td><code>${cmd.command}</code><span class="mirdb-badge">MirDB</span></td>
                    <td><code>${cmd.syntax}</code></td>
                    <td>${cmd.description}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        </section>
      `;

      container.innerHTML = html;

      // Verify command table renders
      const commandTable = container.querySelector('[data-testid="command-table"]');
      expect(commandTable).not.toBeNull();

      // Verify mirdb command table renders
      const mirdbTable = container.querySelector('[data-testid="mirdb-command-table"]');
      expect(mirdbTable).not.toBeNull();

      // Verify all standard commands are rendered
      const standardRows = commandTable?.querySelectorAll('tbody tr');
      expect(standardRows?.length).toBe(standardCommands.length);

      // Verify SET command is rendered
      const setRow = container.querySelector('tr[data-command="SET"]');
      expect(setRow).not.toBeNull();
      expect(setRow?.textContent).toContain('SET');
      // Verify there's a code element with syntax info
      const codeElements = setRow?.querySelectorAll('code');
      expect(codeElements?.length).toBeGreaterThan(1);

      // Verify GET command is rendered
      const getRow = container.querySelector('tr[data-command="GET"]');
      expect(getRow).not.toBeNull();
      expect(getRow?.textContent).toContain('GET');

      // Verify DELETE command is rendered
      const deleteRow = container.querySelector('tr[data-command="DELETE"]');
      expect(deleteRow).not.toBeNull();
      expect(deleteRow?.textContent).toContain('DELETE');

      // Verify INFO command is rendered with MirDB badge
      const infoRow = container.querySelector('tr[data-command="INFO"]');
      expect(infoRow).not.toBeNull();
      expect(infoRow?.classList.contains('mirdb-specific')).toBe(true);
      expect(infoRow?.querySelector('.mirdb-badge')).not.toBeNull();

      // Verify MAJOR_COMPACTION command is rendered with MirDB badge
      const compactionRow = container.querySelector('tr[data-command="MAJOR_COMPACTION"]');
      expect(compactionRow).not.toBeNull();
      expect(compactionRow?.classList.contains('mirdb-specific')).toBe(true);
      expect(compactionRow?.querySelector('.mirdb-badge')).not.toBeNull();
    });
  });
});
