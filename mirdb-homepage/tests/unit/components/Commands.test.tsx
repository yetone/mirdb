/**
 * Unit tests for SupportedCommands component.
 * Owner: Scenario 7 - Supported Commands Table
 *
 * Tests cover:
 * - Rendering the commands table with all 8 commands
 * - Verifying each command's name, description, and syntax
 * - Table structure and accessibility
 */

import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { SupportedCommands, CommandRow } from '../../../src/components/Commands/SupportedCommands';
import commandsData from '../../../src/content/commands.json';
import type { Command } from '../../../src/types/index';

const commands: Command[] = commandsData;

describe('SupportedCommands', () => {
  describe('Test Case 1: Render SupportedCommands component', () => {
    it('renders table displaying all 8 supported Memcached commands', () => {
      render(<SupportedCommands />);

      // Check that the commands section is rendered
      const section = screen.getByTestId('commands-section');
      expect(section).toBeInTheDocument();

      // Check that the table is rendered
      const table = screen.getByTestId('commands-table');
      expect(table).toBeInTheDocument();

      // Check that all 8 command rows are rendered
      const expectedCommands = ['get', 'gets', 'set', 'add', 'replace', 'append', 'prepend', 'delete'];
      expectedCommands.forEach(cmdName => {
        expect(screen.getByTestId(`command-row-${cmdName}`)).toBeInTheDocument();
      });

      // Verify exactly 8 rows (excluding header)
      const rows = screen.getAllByTestId(/^command-row-/);
      expect(rows).toHaveLength(8);
    });

    it('displays section heading', () => {
      render(<SupportedCommands />);

      expect(screen.getByRole('heading', { level: 2 })).toHaveTextContent('Supported Commands');
    });
  });

  describe('Test Case 2: Check get command entry', () => {
    it("shows 'get' command with description for retrieving values", () => {
      render(<SupportedCommands />);

      // Check command name
      const commandName = screen.getByTestId('command-name-get');
      expect(commandName).toHaveTextContent('get');

      // Check description contains key phrases about retrieving values
      const description = screen.getByTestId('command-description-get');
      expect(description).toBeInTheDocument();
      expect(description.textContent?.toLowerCase()).toContain('retrieve');
      expect(description.textContent?.toLowerCase()).toContain('value');

      // Check syntax
      const syntax = screen.getByTestId('command-syntax-get');
      expect(syntax).toHaveTextContent('get <key>');
    });
  });

  describe('Test Case 3: Check gets command entry', () => {
    it("shows 'gets' command with CAS token description", () => {
      render(<SupportedCommands />);

      // Check command name
      const commandName = screen.getByTestId('command-name-gets');
      expect(commandName).toHaveTextContent('gets');

      // Check description contains CAS token reference
      const description = screen.getByTestId('command-description-gets');
      expect(description).toBeInTheDocument();
      expect(description.textContent?.toLowerCase()).toContain('cas');

      // Check syntax
      const syntax = screen.getByTestId('command-syntax-gets');
      expect(syntax).toHaveTextContent('gets <key>');
    });
  });

  describe('Test Case 4: Check set command entry', () => {
    it("shows 'set' command with description for storing values", () => {
      render(<SupportedCommands />);

      // Check command name
      const commandName = screen.getByTestId('command-name-set');
      expect(commandName).toHaveTextContent('set');

      // Check description contains key phrases about storing values
      const description = screen.getByTestId('command-description-set');
      expect(description).toBeInTheDocument();
      expect(description.textContent?.toLowerCase()).toContain('store');

      // Check syntax
      const syntax = screen.getByTestId('command-syntax-set');
      expect(syntax).toHaveTextContent('set <key>');
    });
  });

  describe('Test Case 5: Check all 8 commands present', () => {
    it('table contains get, gets, set, add, replace, append, prepend, delete', () => {
      render(<SupportedCommands />);

      const requiredCommands = ['get', 'gets', 'set', 'add', 'replace', 'append', 'prepend', 'delete'];

      requiredCommands.forEach(cmdName => {
        // Verify row exists
        const row = screen.getByTestId(`command-row-${cmdName}`);
        expect(row).toBeInTheDocument();

        // Verify name cell exists
        const nameCell = screen.getByTestId(`command-name-${cmdName}`);
        expect(nameCell).toHaveTextContent(cmdName);

        // Verify description cell exists
        const descCell = screen.getByTestId(`command-description-${cmdName}`);
        expect(descCell).toBeInTheDocument();
        expect(descCell.textContent?.length).toBeGreaterThan(0);

        // Verify syntax cell exists
        const syntaxCell = screen.getByTestId(`command-syntax-${cmdName}`);
        expect(syntaxCell).toBeInTheDocument();
        expect(syntaxCell.textContent?.length).toBeGreaterThan(0);
      });
    });

    it('displays exactly 8 commands, no more, no less', () => {
      render(<SupportedCommands />);

      const commandRows = screen.getAllByTestId(/^command-row-/);
      expect(commandRows).toHaveLength(8);
    });
  });
});

describe('CommandRow', () => {
  it('renders a single command row with name, description, and syntax', () => {
    const command = commands[0];
    render(
      <table>
        <tbody>
          <CommandRow command={command} index={0} />
        </tbody>
      </table>
    );

    expect(screen.getByTestId(`command-row-${command.name}`)).toBeInTheDocument();
    expect(screen.getByTestId(`command-name-${command.name}`)).toHaveTextContent(command.name);
    expect(screen.getByTestId(`command-description-${command.name}`)).toHaveTextContent(command.description);
    expect(screen.getByTestId(`command-syntax-${command.name}`)).toHaveTextContent(command.syntax);
  });

  it('applies alternating row styles based on index', () => {
    const command = commands[0];

    // Even index row
    const { rerender } = render(
      <table>
        <tbody>
          <CommandRow command={command} index={0} />
        </tbody>
      </table>
    );
    let row = screen.getByTestId(`command-row-${command.name}`);
    expect(row).toHaveClass('bg-background');

    // Odd index row
    rerender(
      <table>
        <tbody>
          <CommandRow command={command} index={1} />
        </tbody>
      </table>
    );
    row = screen.getByTestId(`command-row-${command.name}`);
    expect(row).toHaveClass('bg-surface/50');
  });
});

describe('Table Structure', () => {
  it('has proper table headers', () => {
    render(<SupportedCommands />);

    const table = screen.getByTestId('commands-table');
    const headers = within(table).getAllByRole('columnheader');

    expect(headers).toHaveLength(3);
    expect(headers[0]).toHaveTextContent('Command');
    expect(headers[1]).toHaveTextContent('Description');
    expect(headers[2]).toHaveTextContent('Syntax');
  });

  it('has a scrollable container for mobile responsiveness', () => {
    render(<SupportedCommands />);

    const container = screen.getByTestId('commands-table-container');
    expect(container).toBeInTheDocument();
    expect(container).toHaveClass('overflow-x-auto');
  });

  it('table has minimum width for proper display', () => {
    render(<SupportedCommands />);

    const table = screen.getByTestId('commands-table');
    expect(table).toHaveClass('min-w-[500px]');
  });
});

describe('Command Descriptions', () => {
  it('add command describes storing only if key does not exist', () => {
    render(<SupportedCommands />);

    const description = screen.getByTestId('command-description-add');
    expect(description.textContent?.toLowerCase()).toContain('only if');
    expect(description.textContent?.toLowerCase()).toContain('does not');
  });

  it('replace command describes storing only if key exists', () => {
    render(<SupportedCommands />);

    const description = screen.getByTestId('command-description-replace');
    expect(description.textContent?.toLowerCase()).toContain('only if');
    expect(description.textContent?.toLowerCase()).toContain('exists');
  });

  it('append command describes adding data to end', () => {
    render(<SupportedCommands />);

    const description = screen.getByTestId('command-description-append');
    expect(description.textContent?.toLowerCase()).toContain('append');
    expect(description.textContent?.toLowerCase()).toContain('end');
  });

  it('prepend command describes adding data to beginning', () => {
    render(<SupportedCommands />);

    const description = screen.getByTestId('command-description-prepend');
    expect(description.textContent?.toLowerCase()).toContain('prepend');
    expect(description.textContent?.toLowerCase()).toContain('beginning');
  });

  it('delete command describes removing key-value pair', () => {
    render(<SupportedCommands />);

    const description = screen.getByTestId('command-description-delete');
    expect(description.textContent?.toLowerCase()).toContain('remove');
  });
});
