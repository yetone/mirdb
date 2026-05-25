/**
 * Unit/Integration tests for InteractiveDemo component.
 * Covers REQ-3 (interactive demo) and REQ-14 (Quick Try) - Scenario 11.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import InteractiveDemo, {
  parseCommand,
  isValidCommand,
  formatResponse,
  executeCommand,
} from '../../../src/components/interactive-demo/InteractiveDemo';
import * as client from '../../../src/api/client';
import type { KVOperationResponse } from '../../../src/types';

describe('InteractiveDemo', () => {
  const executeOperationSpy = vi.spyOn(client, 'executeOperation');

  beforeEach(() => {
    executeOperationSpy.mockClear();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('rendering', () => {
    it('renders the interactive demo section', () => {
      render(<InteractiveDemo />);
      expect(screen.getByTestId('interactive-demo')).toBeInTheDocument();
    });

    it('renders the section heading', () => {
      render(<InteractiveDemo />);
      expect(screen.getByTestId('interactive-demo-heading')).toHaveTextContent(
        'Interactive Demo'
      );
    });

    it('renders the description text', () => {
      render(<InteractiveDemo />);
      expect(screen.getByTestId('interactive-demo-description')).toHaveTextContent(
        'Try MirDB commands directly in your browser. No installation required.'
      );
    });

    it('renders the command input field', () => {
      render(<InteractiveDemo />);
      const input = screen.getByTestId('command-input');
      expect(input).toBeInTheDocument();
      expect(input).toHaveAttribute('type', 'text');
      expect(input).toHaveAttribute(
        'placeholder',
        'Enter a memcached command...'
      );
    });

    it('renders the execute button', () => {
      render(<InteractiveDemo />);
      const button = screen.getByTestId('execute-button');
      expect(button).toBeInTheDocument();
      expect(button).toHaveTextContent('Execute');
    });

    it('renders the output terminal', () => {
      render(<InteractiveDemo />);
      expect(screen.getByTestId('output-terminal')).toBeInTheDocument();
    });

    it('shows placeholder when no commands have been run', () => {
      render(<InteractiveDemo />);
      expect(screen.getByTestId('terminal-placeholder')).toHaveTextContent(
        'Type a command and press Enter to see the result...'
      );
    });

    it('renders the command hint', () => {
      render(<InteractiveDemo />);
      expect(screen.getByTestId('command-input-hint')).toHaveTextContent(
        'Try: set key value | get key | delete key | flush_all'
      );
    });
  });

  describe('accessibility', () => {
    it('has proper ARIA label on section', () => {
      render(<InteractiveDemo />);
      expect(screen.getByTestId('interactive-demo')).toHaveAttribute(
        'aria-label',
        'Interactive Memcached Demo'
      );
    });

    it('input has accessible label', () => {
      render(<InteractiveDemo />);
      expect(screen.getByTestId('command-input')).toHaveAttribute(
        'aria-label',
        'Memcached command input'
      );
    });

    it('terminal has role log', () => {
      render(<InteractiveDemo />);
      expect(screen.getByTestId('output-terminal')).toHaveAttribute(
        'role',
        'log'
      );
    });
  });

  describe('TC-1: SET command', () => {
    it('executes set command and displays STORED response', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      } as KVOperationResponse);

      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'set demo_key demo_value'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        const lines = screen.getAllByTestId('terminal-line-command');
        expect(lines).toHaveLength(1);
        expect(lines[0]).toHaveTextContent('set demo_key demo_value');
      });

      await waitFor(() => {
        const responseLines = screen.getAllByTestId('terminal-line-response');
        expect(responseLines[responseLines.length - 1]).toHaveTextContent(
          'STORED'
        );
      });
    });

    it('preserves command history after multiple commands', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      } as KVOperationResponse);

      render(<InteractiveDemo />);

      await user.type(screen.getByTestId('command-input'), 'set key1 val1');
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        expect(screen.getAllByTestId('terminal-line-command')).toHaveLength(1);
      });

      await user.type(screen.getByTestId('command-input'), 'set key2 val2');
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        const commands = screen.getAllByTestId('terminal-line-command');
        expect(commands).toHaveLength(2);
        expect(commands[0]).toHaveTextContent('set key1 val1');
        expect(commands[1]).toHaveTextContent('set key2 val2');
      });
    });

    it('calls executeOperation with correct params for set', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      } as KVOperationResponse);

      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'set demo_key demo_value'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        expect(executeOperationSpy).toHaveBeenCalledWith({
          op: 'set',
          key: 'demo_key',
          value: 'demo_value',
          flags: 0,
          exptime: 0,
        });
      });
    });

    it('executes set command on Enter key press', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      } as KVOperationResponse);

      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'set hello world'
      );
      await user.keyboard('{Enter}');

      await waitFor(() => {
        const responses = screen.getAllByTestId('terminal-line-response');
        expect(responses[responses.length - 1]).toHaveTextContent('STORED');
      });
    });

    it('handles set with multi-word value', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      } as KVOperationResponse);

      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'set key hello world foo bar'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        expect(executeOperationSpy).toHaveBeenCalledWith(
          expect.objectContaining({
            op: 'set',
            key: 'key',
            value: 'hello world foo bar',
          })
        );
      });
    });

    it('clears input after executing command', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      } as KVOperationResponse);

      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'set key value'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        expect(screen.getByTestId('command-input')).toHaveValue('');
      });
    });

    it('shows set usage error when missing arguments', async () => {
      const user = userEvent.setup();
      render(<InteractiveDemo />);

      await user.type(screen.getByTestId('command-input'), 'set keyonly');
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        const errors = screen.getAllByTestId('terminal-line-error');
        expect(errors[errors.length - 1]).toHaveTextContent(
          'CLIENT_ERROR Usage: set <key> <value>'
        );
      });
    });
  });

  describe('TC-2: GET command', () => {
    it('executes get command and displays value', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
        value: 'demo_value',
      } as KVOperationResponse);

      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'get demo_key'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        const commands = screen.getAllByTestId('terminal-line-command');
        expect(commands[commands.length - 1]).toHaveTextContent(
          'get demo_key'
        );
      });

      await waitFor(() => {
        const responses = screen.getAllByTestId('terminal-line-response');
        expect(responses[responses.length - 1]).toHaveTextContent(
          'demo_value'
        );
      });
    });

    it('displays END for not_found get response', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'not_found',
        message: 'Key not found',
      } as KVOperationResponse);

      render(<InteractiveDemo />);

      await user.type(screen.getByTestId('command-input'), 'get missing_key');
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        const responses = screen.getAllByTestId('terminal-line-response');
        expect(responses[responses.length - 1]).toHaveTextContent('END');
      });
    });

    it('calls executeOperation with correct params for get', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
        value: 'demo_value',
      } as KVOperationResponse);

      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'get demo_key'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        expect(executeOperationSpy).toHaveBeenCalledWith({
          op: 'get',
          key: 'demo_key',
        });
      });
    });

    it('shows get usage error when missing key', async () => {
      const user = userEvent.setup();
      render(<InteractiveDemo />);

      await user.type(screen.getByTestId('command-input'), 'get');
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        const errors = screen.getAllByTestId('terminal-line-error');
        expect(errors[errors.length - 1]).toHaveTextContent(
          'CLIENT_ERROR Usage: get <key>'
        );
      });
    });
  });

  describe('TC-3: Invalid command handling', () => {
    it('displays ERROR for invalid command', async () => {
      const user = userEvent.setup();
      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'invalid_cmd'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        const errors = screen.getAllByTestId('terminal-line-error');
        expect(errors[0]).toHaveTextContent('ERROR');
      });
    });

    it('displays helpful hint after invalid command', async () => {
      const user = userEvent.setup();
      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'invalid_cmd'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        const infos = screen.getAllByTestId('terminal-line-info');
        expect(infos[0]).toHaveTextContent(
          'Hint: Try set <key> <value>, get <key>, delete <key>, or flush_all'
        );
      });
    });

    it('does not crash on invalid command', async () => {
      const user = userEvent.setup();
      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'crash_me_now!!!'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        expect(screen.getByTestId('interactive-demo')).toBeInTheDocument();
      });
      expect(screen.getByTestId('command-input')).toBeInTheDocument();
      expect(screen.getByTestId('execute-button')).toBeInTheDocument();
    });

    it('displays ERROR for unknown command with extra args', async () => {
      const user = userEvent.setup();
      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'unknown_command arg1 arg2'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        const errors = screen.getAllByTestId('terminal-line-error');
        expect(errors[0]).toHaveTextContent('ERROR');
      });
    });
  });

  describe('TC-4: Ctrl+L clear shortcut', () => {
    it('clears command history when Ctrl+L is pressed', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      } as KVOperationResponse);

      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'set key value'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        expect(screen.getAllByTestId('terminal-line-command')).toHaveLength(1);
      });

      await user.click(screen.getByTestId('command-input'));
      await user.keyboard('{Control>}l{/Control}');

      await waitFor(() => {
        expect(
          screen.queryByTestId('terminal-line-command')
        ).not.toBeInTheDocument();
      });
    });

    it('shows placeholder after clearing history', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      } as KVOperationResponse);

      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'set key value'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        expect(screen.getAllByTestId('terminal-line-command')).toHaveLength(1);
      });

      await user.click(screen.getByTestId('command-input'));
      await user.keyboard('{Control>}l{/Control}');

      await waitFor(() => {
        expect(screen.getByTestId('terminal-placeholder')).toHaveTextContent(
          'Type a command and press Enter to see the result...'
        );
      });
    });
  });

  describe('TC-5: Quick Try - set/get/delete without local server', () => {
    it('executes delete command successfully', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'deleted',
      } as KVOperationResponse);

      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'delete demo_key'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        const responses = screen.getAllByTestId('terminal-line-response');
        expect(responses[responses.length - 1]).toHaveTextContent('DELETED');
      });
    });

    it('executes flush_all command successfully', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockResolvedValue({
        status: 'ok',
      } as KVOperationResponse);

      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'flush_all'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        const responses = screen.getAllByTestId('terminal-line-response');
        expect(responses[responses.length - 1]).toHaveTextContent('OK');
      });
    });

    it('shows connection error when server is unavailable', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockRejectedValue(
        new Error('Failed to fetch')
      );

      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'get key'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        const errors = screen.getAllByTestId('terminal-line-error');
        expect(errors[errors.length - 1]).toHaveTextContent(
          'Failed to fetch'
        );
      });
    });

    it('shows generic connection message for non-Error rejections', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockRejectedValue('some string');

      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'get key'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        const errors = screen.getAllByTestId('terminal-line-error');
        expect(errors[errors.length - 1]).toHaveTextContent(
          'Connection failed. Is the MirDB server running?'
        );
      });
    });

    it('displays clear connection requirements message', async () => {
      const user = userEvent.setup();
      executeOperationSpy.mockRejectedValue(
        new Error('Connection refused')
      );

      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'set key value'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        const errors = screen.getAllByTestId('terminal-line-error');
        expect(errors[errors.length - 1]).toHaveTextContent(
          'Connection refused'
        );
      });
    });
  });

  describe('loading state', () => {
    it('disables input during command execution', async () => {
      const user = userEvent.setup();
      let resolveOp: (value: { status: 'ok' }) => void;
      const promise = new Promise<{ status: 'ok' }>((resolve) => {
        resolveOp = resolve;
      });
      executeOperationSpy.mockReturnValue(promise as Promise<KVOperationResponse>);

      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'set key value'
      );
      await user.click(screen.getByTestId('execute-button'));

      expect(screen.getByTestId('command-input')).toBeDisabled();
      expect(screen.getByTestId('execute-button')).toBeDisabled();

      resolveOp!({ status: 'ok' });

      await waitFor(() => {
        expect(screen.getByTestId('command-input')).not.toBeDisabled();
      });
    });

    it('shows running text on button during execution', async () => {
      const user = userEvent.setup();
      let resolveOp: (value: { status: 'ok' }) => void;
      const promise = new Promise<{ status: 'ok' }>((resolve) => {
        resolveOp = resolve;
      });
      executeOperationSpy.mockReturnValue(promise as Promise<KVOperationResponse>);

      render(<InteractiveDemo />);

      await user.type(
        screen.getByTestId('command-input'),
        'set key value'
      );
      await user.click(screen.getByTestId('execute-button'));

      expect(screen.getByTestId('execute-button')).toHaveTextContent(
        'Running...'
      );

      resolveOp!({ status: 'ok' });

      await waitFor(() => {
        expect(screen.getByTestId('execute-button')).toHaveTextContent(
          'Execute'
        );
      });
    });
  });

  describe('onExecuteCommand prop', () => {
    it('uses custom onExecuteCommand when provided', async () => {
      const user = userEvent.setup();
      const onExecuteCommand = vi
        .fn()
        .mockResolvedValue('CUSTOM_RESPONSE');

      render(<InteractiveDemo onExecuteCommand={onExecuteCommand} />);

      await user.type(
        screen.getByTestId('command-input'),
        'set key value'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        expect(onExecuteCommand).toHaveBeenCalledWith('set', [
          'key',
          'value',
        ]);
      });

      await waitFor(() => {
        const responses = screen.getAllByTestId('terminal-line-response');
        expect(responses[responses.length - 1]).toHaveTextContent(
          'CUSTOM_RESPONSE'
        );
      });

      expect(executeOperationSpy).not.toHaveBeenCalled();
    });

    it('passes correct args for get command to custom handler', async () => {
      const user = userEvent.setup();
      const onExecuteCommand = vi.fn().mockResolvedValue('VALUE hello');

      render(<InteractiveDemo onExecuteCommand={onExecuteCommand} />);

      await user.type(
        screen.getByTestId('command-input'),
        'get hello'
      );
      await user.click(screen.getByTestId('execute-button'));

      await waitFor(() => {
        expect(onExecuteCommand).toHaveBeenCalledWith('get', ['hello']);
      });
    });
  });
});

describe('parseCommand', () => {
  it('parses a simple command', () => {
    expect(parseCommand('set key value')).toEqual({
      command: 'set',
      args: ['key', 'value'],
    });
  });

  it('handles extra whitespace', () => {
    expect(parseCommand('  set   key    value  ')).toEqual({
      command: 'set',
      args: ['key', 'value'],
    });
  });

  it('handles empty input', () => {
    expect(parseCommand('')).toEqual({
      command: '',
      args: [],
    });
  });

  it('converts command to lowercase', () => {
    expect(parseCommand('SET Key Value')).toEqual({
      command: 'set',
      args: ['Key', 'Value'],
    });
  });

  it('handles command with no args', () => {
    expect(parseCommand('flush_all')).toEqual({
      command: 'flush_all',
      args: [],
    });
  });

  it('handles multi-word value', () => {
    expect(parseCommand('set key hello world foo bar')).toEqual({
      command: 'set',
      args: ['key', 'hello', 'world', 'foo', 'bar'],
    });
  });
});

describe('isValidCommand', () => {
  it('returns true for set', () => {
    expect(isValidCommand('set')).toBe(true);
  });

  it('returns true for get', () => {
    expect(isValidCommand('get')).toBe(true);
  });

  it('returns true for delete', () => {
    expect(isValidCommand('delete')).toBe(true);
  });

  it('returns true for flush_all', () => {
    expect(isValidCommand('flush_all')).toBe(true);
  });

  it('returns false for unknown command', () => {
    expect(isValidCommand('unknown')).toBe(false);
  });

  it('returns false for empty string', () => {
    expect(isValidCommand('')).toBe(false);
  });
});

describe('formatResponse', () => {
  it('returns STORED for successful set', () => {
    expect(formatResponse({ status: 'ok' }, 'set')).toBe('STORED');
  });

  it('returns value for successful get', () => {
    expect(formatResponse({ status: 'ok', value: 'hello' }, 'get')).toBe(
      'hello'
    );
  });

  it('returns END for get with no value', () => {
    expect(formatResponse({ status: 'ok' }, 'get')).toBe('END');
  });

  it('returns DELETED for delete', () => {
    expect(formatResponse({ status: 'deleted' }, 'delete')).toBe('DELETED');
  });

  it('returns NOT_FOUND for delete not_found', () => {
    expect(formatResponse({ status: 'not_found' }, 'delete')).toBe('NOT_FOUND');
  });

  it('returns END for get not_found', () => {
    expect(formatResponse({ status: 'not_found' }, 'get')).toBe('END');
  });

  it('returns OK for flush_all', () => {
    expect(formatResponse({ status: 'ok' }, 'flush_all')).toBe('OK');
  });

  it('returns error message', () => {
    expect(
      formatResponse({ status: 'error', message: 'Server error' }, 'set')
    ).toBe('Server error');
  });

  it('returns ERROR for error with no message', () => {
    expect(formatResponse({ status: 'error' }, 'set')).toBe('ERROR');
  });
});
