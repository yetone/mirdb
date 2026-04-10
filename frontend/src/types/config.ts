/**
 * Configuration type definitions
 *
 * Owner: Scenario 3 - Configuration Panel View and Edit
 */

export interface ConfigOption {
  name: string;
  value: string | number | boolean;
  type: 'string' | 'number' | 'boolean';
  description: string;
  requires_restart: boolean;
}

export interface ConfigResponse {
  options: ConfigOption[];
}
