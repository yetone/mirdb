/**
 * React version of SupportedCommands for testing purposes.
 * This mirrors the SupportedCommands.astro component structure.
 * Owner: Scenario 7 - Supported Commands Table
 */

import type { Command } from '../../types/index';

interface CommandRowProps {
  command: Command;
  index: number;
}

export function CommandRow({ command, index }: CommandRowProps) {
  return (
    <tr
      className={`${index % 2 === 0 ? 'bg-background' : 'bg-surface/50'} hover:bg-surface transition-colors`}
      data-testid={`command-row-${command.name}`}
    >
      <td className="px-6 py-4 border-b border-border">
        <code className="text-accent font-mono font-semibold" data-testid={`command-name-${command.name}`}>
          {command.name}
        </code>
      </td>
      <td className="px-6 py-4 text-text-secondary text-sm border-b border-border" data-testid={`command-description-${command.name}`}>
        {command.description}
      </td>
      <td className="px-6 py-4 border-b border-border">
        <code className="text-text-primary font-mono text-xs bg-surface px-2 py-1 rounded" data-testid={`command-syntax-${command.name}`}>
          {command.syntax}
        </code>
      </td>
    </tr>
  );
}

interface SupportedCommandsProps {
  commands?: Command[];
}

// Default commands matching the content/commands.json
const defaultCommands: Command[] = [
  {
    name: 'get',
    description: 'Retrieve the value associated with a key. Returns the value if found, or nothing if the key does not exist.',
    syntax: 'get <key>',
  },
  {
    name: 'gets',
    description: 'Retrieve the value and CAS (Check-And-Set) token for a key. Used for optimistic locking to prevent race conditions.',
    syntax: 'gets <key>',
  },
  {
    name: 'set',
    description: 'Store a key-value pair. Overwrites any existing value for the key with the new value.',
    syntax: 'set <key> <flags> <exptime> <bytes>',
  },
  {
    name: 'add',
    description: 'Store a key-value pair only if the key does not already exist. Fails if the key is already present.',
    syntax: 'add <key> <flags> <exptime> <bytes>',
  },
  {
    name: 'replace',
    description: 'Store a key-value pair only if the key already exists. Fails if the key is not present.',
    syntax: 'replace <key> <flags> <exptime> <bytes>',
  },
  {
    name: 'append',
    description: "Append data to an existing key's value. Adds the new data to the end of the current value.",
    syntax: 'append <key> <flags> <exptime> <bytes>',
  },
  {
    name: 'prepend',
    description: "Prepend data to an existing key's value. Adds the new data to the beginning of the current value.",
    syntax: 'prepend <key> <flags> <exptime> <bytes>',
  },
  {
    name: 'delete',
    description: 'Remove a key-value pair from the store. Returns success if the key was found and deleted.',
    syntax: 'delete <key>',
  },
];

export function SupportedCommands({ commands = defaultCommands }: SupportedCommandsProps) {
  return (
    <section id="commands" className="py-16 px-4" data-testid="commands-section">
      <div className="max-w-4xl mx-auto">
        <h2 className="text-3xl font-mono font-bold text-center mb-8 text-text-primary">
          Supported Commands
        </h2>
        <p className="text-text-secondary text-center mb-8 max-w-2xl mx-auto">
          MirDB supports the following Memcached protocol commands, providing full compatibility with existing clients and libraries.
        </p>

        {/* Mobile-friendly scrollable container */}
        <div className="overflow-x-auto rounded-lg border border-border" data-testid="commands-table-container">
          <table className="w-full min-w-[500px]" data-testid="commands-table">
            <thead className="bg-surface">
              <tr>
                <th className="px-6 py-4 text-left text-sm font-mono font-semibold text-text-primary border-b border-border">
                  Command
                </th>
                <th className="px-6 py-4 text-left text-sm font-mono font-semibold text-text-primary border-b border-border">
                  Description
                </th>
                <th className="px-6 py-4 text-left text-sm font-mono font-semibold text-text-primary border-b border-border">
                  Syntax
                </th>
              </tr>
            </thead>
            <tbody>
              {commands.map((command, index) => (
                <CommandRow key={command.name} command={command} index={index} />
              ))}
            </tbody>
          </table>
        </div>

        <p className="text-text-secondary text-sm text-center mt-6">
          All commands follow the standard Memcached text protocol specification.
        </p>
      </div>
    </section>
  );
}

export default SupportedCommands;
