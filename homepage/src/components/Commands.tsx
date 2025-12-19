import React from 'react';

export interface Command {
  name: string;
  syntax: string;
  description: string;
  response: string;
}

export const commandsData: Command[] = [
  {
    name: 'get',
    syntax: 'get <key>',
    description: 'Retrieve the value associated with the specified key',
    response: 'VALUE <key> <flags> <bytes>\\r\\n<data>\\r\\nEND',
  },
  {
    name: 'set',
    syntax: 'set <key> <flags> <exptime> <bytes>',
    description: 'Store a key-value pair with optional expiration time',
    response: 'STORED',
  },
  {
    name: 'delete',
    syntax: 'delete <key>',
    description: 'Remove the specified key from the store',
    response: 'DELETED',
  },
  {
    name: 'add',
    syntax: 'add <key> <flags> <exptime> <bytes>',
    description: 'Store a key-value pair only if the key does not already exist',
    response: 'STORED or NOT_STORED',
  },
  {
    name: 'replace',
    syntax: 'replace <key> <flags> <exptime> <bytes>',
    description: 'Store a key-value pair only if the key already exists',
    response: 'STORED or NOT_STORED',
  },
  {
    name: 'append',
    syntax: 'append <key> <flags> <exptime> <bytes>',
    description: 'Append data to an existing key value',
    response: 'STORED or NOT_STORED',
  },
  {
    name: 'prepend',
    syntax: 'prepend <key> <flags> <exptime> <bytes>',
    description: 'Prepend data to an existing key value',
    response: 'STORED or NOT_STORED',
  },
];

export const Commands: React.FC = () => {
  return (
    <section id="commands" className="commands-section" data-testid="commands-section">
      <div className="container">
        <h2 className="section-title">Supported Commands</h2>
        <div className="commands-table-wrapper">
          <table className="commands-table" data-testid="commands-table">
            <thead>
              <tr>
                <th>Command</th>
                <th>Syntax</th>
                <th>Description</th>
                <th>Response</th>
              </tr>
            </thead>
            <tbody>
              {commandsData.map((command) => (
                <tr key={command.name} data-testid={`command-${command.name}`}>
                  <td className="command-name">{command.name}</td>
                  <td className="command-syntax">
                    <code>{command.syntax}</code>
                  </td>
                  <td className="command-description">{command.description}</td>
                  <td className="command-response">
                    <code>{command.response}</code>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
};

export default Commands;
