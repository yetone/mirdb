/**
 * Query API client for memcached command execution
 *
 * Owner: Scenario 6 - Interactive Query Builder
 *
 * Provides methods for:
 * - Executing memcached commands via POST /api/query
 */

import { apiClient } from './index';
import type { QueryRequest, QueryResponse } from '../types/api';

/**
 * Execute a memcached command through the API
 * @param command The memcached command to execute (e.g., 'stats', 'get mykey')
 * @returns Promise resolving to the query response
 */
export async function executeQuery(command: string): Promise<QueryResponse> {
  const request: QueryRequest = { command };
  return apiClient.post<QueryResponse>('/query', request);
}

/**
 * Validate a memcached command syntax (client-side check)
 * @param command The command to validate
 * @returns Object with isValid flag and optional error message
 */
export function validateCommand(command: string): { isValid: boolean; error?: string } {
  const trimmed = command.trim();

  if (!trimmed) {
    return { isValid: false, error: 'Command cannot be empty' };
  }

  // Basic validation for known commands
  const validCommands = [
    'stats', 'get', 'set', 'add', 'replace', 'append', 'prepend',
    'delete', 'incr', 'decr', 'touch', 'flush_all', 'version', 'quit'
  ];

  const firstWord = trimmed.split(/\s+/)[0].toLowerCase();

  if (!validCommands.includes(firstWord)) {
    return { isValid: false, error: `Unknown command: ${firstWord}` };
  }

  return { isValid: true };
}
