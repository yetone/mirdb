/**
 * Unit tests for QueryBuilder component
 *
 * Owner: Scenario 6 - Interactive Query Builder
 *
 * Test Case 4: Render QueryBuilder component
 * Expected: Component displays input field, execute button, and response area
 *
 * Test Case 5: Execute command and check history
 * Expected: Command appears in history list and can be clicked to re-execute
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryBuilder } from '../../../../src/components/query/QueryBuilder';
import * as queryApi from '../../../../src/api/query';

// Mock the query API
vi.mock('../../../../src/api/query', () => ({
  executeQuery: vi.fn(),
  validateCommand: vi.fn(() => ({ isValid: true })),
}));

describe('QueryBuilder', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  /**
   * Test Case 4: Render QueryBuilder component
   * Verifies that the component displays:
   * - Input field for commands
   * - Execute button
   * - Response display area
   */
  it('should render input field, execute button, and response area', () => {
    render(<QueryBuilder />);

    // Check for main container
    expect(screen.getByTestId('query-builder')).toBeInTheDocument();

    // Check for input field
    expect(screen.getByTestId('query-command-input')).toBeInTheDocument();

    // Check for execute button
    expect(screen.getByTestId('query-execute-button')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /execute/i })).toBeInTheDocument();

    // Check for response area
    expect(screen.getByTestId('query-response-area')).toBeInTheDocument();
    expect(screen.getByTestId('query-response')).toBeInTheDocument();
  });

  it('should render query history section', () => {
    render(<QueryBuilder />);

    // Check for history section
    expect(screen.getByTestId('query-history')).toBeInTheDocument();
  });

  it('should show empty state message in response area initially', () => {
    render(<QueryBuilder />);

    expect(
      screen.getByText(/enter a command above and click execute/i)
    ).toBeInTheDocument();
  });

  it('should disable execute button when input is empty', () => {
    render(<QueryBuilder />);

    const executeButton = screen.getByTestId('query-execute-button');
    expect(executeButton).toBeDisabled();
  });

  it('should enable execute button when input has content', async () => {
    const user = userEvent.setup();
    render(<QueryBuilder />);

    const input = screen.getByTestId('query-command-input');
    await user.type(input, 'stats');

    const executeButton = screen.getByTestId('query-execute-button');
    expect(executeButton).toBeEnabled();
  });

  /**
   * Test Case 5: Execute command and check history
   * Verifies that:
   * - Command appears in history list after execution
   * - History item can be clicked to re-execute
   */
  it('should execute command and add to history', async () => {
    const mockResponse = {
      response: 'STAT version 1.0.0\r\nEND',
      success: true,
      execution_time_ms: 5,
    };

    vi.mocked(queryApi.executeQuery).mockResolvedValueOnce(mockResponse);

    const user = userEvent.setup();
    render(<QueryBuilder />);

    // Type a command
    const input = screen.getByTestId('query-command-input');
    await user.type(input, 'stats');

    // Click execute
    const executeButton = screen.getByTestId('query-execute-button');
    await user.click(executeButton);

    // Wait for response
    await waitFor(() => {
      expect(queryApi.executeQuery).toHaveBeenCalledWith('stats');
    });

    // Check response is displayed
    await waitFor(() => {
      const responseContent = screen.getByTestId('query-response-content');
      expect(responseContent).toHaveTextContent('STAT version 1.0.0');
    });

    // Check history shows the command
    await waitFor(() => {
      const historyList = screen.getByTestId('query-history-list');
      expect(historyList).toBeInTheDocument();
      expect(historyList).toHaveTextContent('stats');
    });
  });

  it('should show loading state while executing', async () => {
    // Make executeQuery hang for a while
    vi.mocked(queryApi.executeQuery).mockImplementation(
      () => new Promise((resolve) => setTimeout(resolve, 1000))
    );

    const user = userEvent.setup();
    render(<QueryBuilder />);

    const input = screen.getByTestId('query-command-input');
    await user.type(input, 'stats');

    const executeButton = screen.getByTestId('query-execute-button');
    await user.click(executeButton);

    // Check for loading state - button should show "Executing" and be disabled
    expect(executeButton).toBeDisabled();
    expect(executeButton).toHaveTextContent(/executing/i);
  });

  it('should show error response for failed command', async () => {
    const mockErrorResponse = {
      response: '',
      success: false,
      error: 'Invalid command',
    };

    vi.mocked(queryApi.executeQuery).mockResolvedValueOnce(mockErrorResponse);

    const user = userEvent.setup();
    render(<QueryBuilder />);

    const input = screen.getByTestId('query-command-input');
    await user.type(input, 'invalid_cmd');

    const executeButton = screen.getByTestId('query-execute-button');
    await user.click(executeButton);

    await waitFor(() => {
      expect(screen.getByTestId('query-response-status')).toHaveTextContent('Error');
    });
  });

  it('should re-execute command from history', async () => {
    const mockResponse1 = {
      response: 'STAT version 1.0.0\r\nEND',
      success: true,
      execution_time_ms: 5,
    };

    const mockResponse2 = {
      response: 'STAT version 1.0.1\r\nEND',
      success: true,
      execution_time_ms: 3,
    };

    vi.mocked(queryApi.executeQuery)
      .mockResolvedValueOnce(mockResponse1)
      .mockResolvedValueOnce(mockResponse2);

    const user = userEvent.setup();
    render(<QueryBuilder />);

    // Execute first command
    const input = screen.getByTestId('query-command-input');
    await user.type(input, 'stats');
    const executeButton = screen.getByTestId('query-execute-button');
    await user.click(executeButton);

    // Wait for first response
    await waitFor(() => {
      expect(screen.getByTestId('query-response-content')).toHaveTextContent('version 1.0.0');
    });

    // Find and click history item
    await waitFor(async () => {
      const historyList = screen.getByTestId('query-history-list');
      const historyButton = historyList.querySelector('button');
      expect(historyButton).toBeInTheDocument();
      if (historyButton) {
        await user.click(historyButton);
      }
    });

    // Wait for second response (from re-execution)
    await waitFor(() => {
      expect(queryApi.executeQuery).toHaveBeenCalledTimes(2);
    });
  });

  it('should show empty history message when no commands executed', () => {
    render(<QueryBuilder />);

    expect(
      screen.getByText(/no command history yet/i)
    ).toBeInTheDocument();
  });

  it('should handle API errors gracefully', async () => {
    vi.mocked(queryApi.executeQuery).mockRejectedValueOnce(
      new Error('Network error')
    );

    const user = userEvent.setup();
    render(<QueryBuilder />);

    const input = screen.getByTestId('query-command-input');
    await user.type(input, 'stats');

    const executeButton = screen.getByTestId('query-execute-button');
    await user.click(executeButton);

    await waitFor(() => {
      expect(screen.getByTestId('query-response-status')).toHaveTextContent('Error');
    });
  });
});

describe('QueryInput', () => {
  it('should call onExecute with trimmed command', async () => {
    const handleExecute = vi.fn();
    const user = userEvent.setup();

    // Import QueryInput directly for isolated testing
    const { QueryInput } = await import('../../../../src/components/query/QueryInput');

    render(<QueryInput onExecute={handleExecute} />);

    const input = screen.getByTestId('query-command-input');
    await user.type(input, '  stats  ');

    const executeButton = screen.getByTestId('query-execute-button');
    await user.click(executeButton);

    expect(handleExecute).toHaveBeenCalledWith('stats');
  });

  it('should execute on Ctrl+Enter', async () => {
    const handleExecute = vi.fn();
    const user = userEvent.setup();

    const { QueryInput } = await import('../../../../src/components/query/QueryInput');

    render(<QueryInput onExecute={handleExecute} />);

    const input = screen.getByTestId('query-command-input');
    await user.type(input, 'stats');
    await user.keyboard('{Control>}{Enter}{/Control}');

    expect(handleExecute).toHaveBeenCalledWith('stats');
  });
});

describe('QueryHistory', () => {
  it('should display history items', async () => {
    const { QueryHistory } = await import('../../../../src/components/query/QueryHistory');

    const history = [
      {
        id: '1',
        command: 'stats',
        response: 'OK',
        success: true,
        timestamp: new Date(),
      },
      {
        id: '2',
        command: 'get key1',
        response: 'VALUE key1 0 5\r\nhello\r\nEND',
        success: true,
        timestamp: new Date(),
      },
    ];

    render(<QueryHistory history={history} onReExecute={() => {}} />);

    expect(screen.getByText('stats')).toBeInTheDocument();
    expect(screen.getByText('get key1')).toBeInTheDocument();
  });

  it('should call onReExecute when clicking history item', async () => {
    const { QueryHistory } = await import('../../../../src/components/query/QueryHistory');
    const user = userEvent.setup();
    const handleReExecute = vi.fn();

    const history = [
      {
        id: '1',
        command: 'stats',
        response: 'OK',
        success: true,
        timestamp: new Date(),
      },
    ];

    render(<QueryHistory history={history} onReExecute={handleReExecute} />);

    const historyItem = screen.getByTestId('history-item-1');
    await user.click(historyItem);

    expect(handleReExecute).toHaveBeenCalledWith('stats');
  });
});

describe('QueryResponse', () => {
  it('should show success state for successful response', async () => {
    const { QueryResponse } = await import('../../../../src/components/query/QueryResponse');

    const response = {
      response: 'STORED',
      success: true,
      execution_time_ms: 5,
    };

    render(<QueryResponse response={response} />);

    expect(screen.getByTestId('query-response-status')).toHaveTextContent('Success');
    expect(screen.getByTestId('query-response-content')).toHaveTextContent('STORED');
  });

  it('should show error state for failed response', async () => {
    const { QueryResponse } = await import('../../../../src/components/query/QueryResponse');

    const response = {
      response: '',
      success: false,
      error: 'Invalid command',
    };

    render(<QueryResponse response={response} />);

    expect(screen.getByTestId('query-response-status')).toHaveTextContent('Error');
  });

  it('should show copy button', async () => {
    const { QueryResponse } = await import('../../../../src/components/query/QueryResponse');

    const response = {
      response: 'STORED',
      success: true,
    };

    render(<QueryResponse response={response} />);

    expect(screen.getByTestId('query-response-copy')).toBeInTheDocument();
  });
});
