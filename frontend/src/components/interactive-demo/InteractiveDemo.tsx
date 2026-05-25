/**
 * Interactive Demo component.
 * Owner: Scenario 11 - Interactive Demo and Quick Try
 *
 * Simulates memcached CLI experience with command history and protocol responses.
 * Covers REQ-3 (interactive demo) and REQ-14 (Quick Try).
 */

import { useState, useCallback, useRef } from 'react';
import { executeOperation } from '../../api/client';
import type { KVOperationResponse } from '../../types';
import CommandInput from './CommandInput';
import OutputTerminal, { type HistoryEntry } from './OutputTerminal';

export interface ParsedCommand {
  command: string;
  args: string[];
}

export interface InteractiveDemoProps {
  onExecuteCommand?: (command: string, args: string[]) => Promise<string>;
}

let entryIdCounter = 0;
function nextId(): string {
  return `entry-${++entryIdCounter}`;
}

export function parseCommand(input: string): ParsedCommand {
  const trimmed = input.trim();
  const parts = trimmed.split(/\s+/);
  return {
    command: parts[0]?.toLowerCase() || '',
    args: parts.slice(1),
  };
}

export function isValidCommand(command: string): boolean {
  return ['set', 'get', 'delete', 'flush_all'].includes(command);
}

export async function executeCommand(
  parsed: ParsedCommand
): Promise<KVOperationResponse> {
  const { command, args } = parsed;

  switch (command) {
    case 'set': {
      if (args.length < 2) {
        return { status: 'error', message: 'CLIENT_ERROR Usage: set <key> <value>' };
      }
      const [key, ...valueParts] = args;
      return executeOperation({
        op: 'set',
        key,
        value: valueParts.join(' '),
        flags: 0,
        exptime: 0,
      });
    }

    case 'get': {
      if (args.length < 1) {
        return { status: 'error', message: 'CLIENT_ERROR Usage: get <key>' };
      }
      return executeOperation({
        op: 'get',
        key: args[0],
      });
    }

    case 'delete': {
      if (args.length < 1) {
        return { status: 'error', message: 'CLIENT_ERROR Usage: delete <key>' };
      }
      return executeOperation({
        op: 'delete',
        key: args[0],
      });
    }

    case 'flush_all': {
      return executeOperation({ op: 'flush_all' });
    }

    default: {
      return { status: 'error', message: 'ERROR' };
    }
  }
}

export function formatResponse(
  response: KVOperationResponse,
  command: string
): string {
  switch (response.status) {
    case 'ok':
      if (command === 'get') {
        return response.value ?? 'END';
      }
      if (command === 'set') {
        return 'STORED';
      }
      if (command === 'flush_all') {
        return 'OK';
      }
      return response.value ?? 'OK';
    case 'not_found':
      if (command === 'delete') {
        return 'NOT_FOUND';
      }
      return 'END';
    case 'deleted':
      return 'DELETED';
    case 'error':
      return response.message ?? 'ERROR';
    default:
      return 'ERROR';
  }
}

export default function InteractiveDemo({ onExecuteCommand }: InteractiveDemoProps) {
  const [input, setInput] = useState('');
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [isExecuting, setIsExecuting] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const addEntry = useCallback((type: HistoryEntry['type'], text: string) => {
    setHistory((prev) => [
      ...prev,
      { id: nextId(), type, text, timestamp: Date.now() },
    ]);
  }, []);

  const handleClear = useCallback(() => {
    setHistory([]);
    inputRef.current?.focus();
  }, []);

  const handleExecute = useCallback(async () => {
    const trimmed = input.trim();
    if (!trimmed || isExecuting) return;

    setIsExecuting(true);
    addEntry('command', trimmed);
    setInput('');

    const parsed = parseCommand(trimmed);

    if (!isValidCommand(parsed.command)) {
      addEntry('error', `ERROR`);
      addEntry(
        'info',
        'Hint: Try set <key> <value>, get <key>, delete <key>, or flush_all'
      );
      setIsExecuting(false);
      return;
    }

    try {
      let responseText: string;

      if (onExecuteCommand) {
        responseText = await onExecuteCommand(parsed.command, parsed.args);
        addEntry('response', responseText);
      } else {
        const response = await executeCommand(parsed);
        responseText = formatResponse(response, parsed.command);
        if (response.status === 'error') {
          addEntry('error', responseText);
        } else {
          addEntry('response', responseText);
        }
      }
    } catch (err) {
      const message =
        err instanceof Error
          ? err.message
          : 'Connection failed. Is the MirDB server running?';
      addEntry('error', message);
    } finally {
      setIsExecuting(false);
    }
  }, [input, isExecuting, addEntry, onExecuteCommand]);

  return (
    <section
      className="interactive-demo"
      aria-label="Interactive Memcached Demo"
      data-testid="interactive-demo"
    >
      <div className="interactive-demo__container">
        <h2 className="interactive-demo__heading" data-testid="interactive-demo-heading">
          Interactive Demo
        </h2>
        <p className="interactive-demo__description" data-testid="interactive-demo-description">
          Try MirDB commands directly in your browser. No installation required.
        </p>

        <div className="interactive-demo__terminal-wrapper">
          <OutputTerminal history={history} />
        </div>

        <CommandInput
          value={input}
          onChange={setInput}
          onExecute={handleExecute}
          onClear={handleClear}
          disabled={isExecuting}
        />
      </div>
    </section>
  );
}
