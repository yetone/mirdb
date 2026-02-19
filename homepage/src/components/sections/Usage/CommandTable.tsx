/**
 * CommandTable Component
 * Owner: Scenario 5 - Usage Demonstration
 *
 * Displays supported Memcached commands in a table format:
 * - Command name
 * - Syntax
 * - Description
 */

import type { Command } from '@/types'
import styles from './Usage.module.css'

interface CommandTableProps {
  commands: Command[]
}

export function CommandTable({ commands }: CommandTableProps) {
  return (
    <div className={styles.tableWrapper} data-testid="command-table-wrapper">
      <table className={styles.table} data-testid="command-table">
        <thead>
          <tr>
            <th scope="col">Command</th>
            <th scope="col">Syntax</th>
            <th scope="col">Description</th>
          </tr>
        </thead>
        <tbody>
          {commands.map((command) => (
            <tr key={command.name} data-testid={`command-row-${command.name}`}>
              <td className={styles.commandName}>{command.name}</td>
              <td className={styles.commandSyntax}>{command.syntax}</td>
              <td>{command.description}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
