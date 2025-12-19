import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { Commands, commandsData, type Command } from './Commands';

describe('Commands Section - E2E Tests', () => {
  describe('Test Case 1: GET command documentation', () => {
    it('displays GET command syntax and usage example', () => {
      render(<Commands />);

      // Check for the commands section
      const commandsSection = screen.getByTestId('commands-section');
      expect(commandsSection).toBeInTheDocument();

      // Check for GET command
      expect(screen.getByText('get')).toBeInTheDocument();
      expect(screen.getByText(/get <key>/i)).toBeInTheDocument();
    });

    it('GET command has proper description', () => {
      render(<Commands />);

      const getCommand = commandsData.find((cmd) => cmd.name === 'get');
      expect(getCommand).toBeDefined();
      expect(getCommand?.description).toContain('Retrieve');
    });
  });

  describe('Test Case 2: SET command documentation', () => {
    it('displays SET command syntax including flags, exptime, bytes parameters', () => {
      render(<Commands />);

      // Check for SET command
      expect(screen.getByText('set')).toBeInTheDocument();
      expect(screen.getByText(/set <key> <flags> <exptime> <bytes>/i)).toBeInTheDocument();
    });

    it('SET command has proper description mentioning storage', () => {
      render(<Commands />);

      const setCommand = commandsData.find((cmd) => cmd.name === 'set');
      expect(setCommand).toBeDefined();
      expect(setCommand?.description.toLowerCase()).toContain('store');
    });
  });

  describe('Test Case 3: DELETE command documentation', () => {
    it('displays DELETE command syntax and usage example', () => {
      render(<Commands />);

      // Check for DELETE command
      expect(screen.getByText('delete')).toBeInTheDocument();
      expect(screen.getByText(/delete <key>/i)).toBeInTheDocument();
    });

    it('DELETE command has proper description', () => {
      render(<Commands />);

      const deleteCommand = commandsData.find((cmd) => cmd.name === 'delete');
      expect(deleteCommand).toBeDefined();
      expect(deleteCommand?.description.toLowerCase()).toContain('remove');
    });
  });

  describe('Test Case 4: Command examples show expected responses', () => {
    it('displays response format examples like STORED, VALUE, DELETED, END', () => {
      render(<Commands />);

      // Check for response examples (using getAllByText since multiple commands have similar responses)
      expect(screen.getAllByText(/STORED/).length).toBeGreaterThanOrEqual(1);
      expect(screen.getAllByText(/VALUE/).length).toBeGreaterThanOrEqual(1);
      expect(screen.getAllByText(/DELETED/).length).toBeGreaterThanOrEqual(1);
      expect(screen.getAllByText(/END/).length).toBeGreaterThanOrEqual(1);
    });

    it('each command has expected response documented', () => {
      render(<Commands />);

      // Verify commands data contains response information
      const setCommand = commandsData.find((cmd) => cmd.name === 'set');
      expect(setCommand?.response).toBeDefined();
      expect(setCommand?.response).toContain('STORED');

      const getCommand = commandsData.find((cmd) => cmd.name === 'get');
      expect(getCommand?.response).toBeDefined();
      expect(getCommand?.response).toContain('VALUE');
      expect(getCommand?.response).toContain('END');

      const deleteCommand = commandsData.find((cmd) => cmd.name === 'delete');
      expect(deleteCommand?.response).toBeDefined();
      expect(deleteCommand?.response).toContain('DELETED');
    });
  });

  describe('Test Case 5: Commands displayed in structured table or list format', () => {
    it('renders commands in a structured table format', () => {
      render(<Commands />);

      // Check for table element
      const commandsTable = screen.getByTestId('commands-table');
      expect(commandsTable).toBeInTheDocument();

      // Check table has proper headers
      expect(screen.getByText('Command')).toBeInTheDocument();
      expect(screen.getByText('Syntax')).toBeInTheDocument();
      expect(screen.getByText('Description')).toBeInTheDocument();
      expect(screen.getByText('Response')).toBeInTheDocument();
    });

    it('all commands are displayed in the table', () => {
      render(<Commands />);

      const commandsTable = screen.getByTestId('commands-table');

      // Should display all core commands
      expect(within(commandsTable).getByText('get')).toBeInTheDocument();
      expect(within(commandsTable).getByText('set')).toBeInTheDocument();
      expect(within(commandsTable).getByText('delete')).toBeInTheDocument();
    });

    it('has correct number of commands in data', () => {
      // Should have at least GET, SET, DELETE
      expect(commandsData.length).toBeGreaterThanOrEqual(3);

      const commandNames = commandsData.map((cmd) => cmd.name);
      expect(commandNames).toContain('get');
      expect(commandNames).toContain('set');
      expect(commandNames).toContain('delete');
    });
  });

  describe('Commands Section Structure', () => {
    it('has a section title', () => {
      render(<Commands />);

      expect(screen.getByText('Supported Commands')).toBeInTheDocument();
    });

    it('commands section has proper id for navigation', () => {
      render(<Commands />);

      const commandsSection = screen.getByTestId('commands-section');
      expect(commandsSection).toHaveAttribute('id', 'commands');
    });

    it('each command entry has all required fields', () => {
      commandsData.forEach((cmd: Command) => {
        expect(cmd.name).toBeDefined();
        expect(cmd.syntax).toBeDefined();
        expect(cmd.description).toBeDefined();
        expect(cmd.response).toBeDefined();
      });
    });
  });
});

describe('Commands Component - Unit Tests', () => {
  it('renders without crashing', () => {
    render(<Commands />);
    expect(screen.getByTestId('commands-section')).toBeInTheDocument();
  });

  it('exports commandsData array', () => {
    expect(Array.isArray(commandsData)).toBe(true);
    expect(commandsData.length).toBeGreaterThan(0);
  });

  it('exports Command type interface', () => {
    const testCommand: Command = {
      name: 'test',
      syntax: 'test <arg>',
      description: 'Test command',
      response: 'OK',
    };
    expect(testCommand.name).toBe('test');
  });
});
