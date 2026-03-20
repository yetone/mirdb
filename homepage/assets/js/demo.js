/**
 * Interactive Demo Module
 * Owner: Scenario 3 - Interactive Demo Functionality
 *
 * Exports:
 * - initDemo(): Initialize demo terminal
 * - executeCommand(command): Send command to backend
 * - displayOutput(response): Render response in terminal
 * - showError(message): Display error state (Scenario 15)
 * - showLoading(): Show loading indicator
 * - showFallback(): Show static example when backend unavailable (Scenario 15)
 *
 * API endpoint: POST /api/demo/execute
 * Request: { command: string }
 * Response: { output: string, success: boolean }
 */

(function() {
  'use strict';

  // In-memory store for demo (simulates MirDB when backend unavailable)
  const demoStore = new Map();

  // DOM Elements
  let outputEl = null;
  let inputEl = null;
  let formEl = null;
  let submitBtn = null;

  // API Configuration
  const API_ENDPOINT = '/api/demo/execute';
  const USE_BACKEND = false; // Toggle for backend usage
  const SIMULATED_DELAY = 150; // Delay in ms for simulated responses

  /**
   * Initialize the demo terminal
   */
  function initDemo() {
    outputEl = document.getElementById('demo-output');
    inputEl = document.getElementById('demo-input');
    formEl = document.getElementById('demo-form');
    submitBtn = formEl ? formEl.querySelector('.demo__submit') : null;

    if (!formEl || !inputEl || !outputEl) {
      console.warn('Demo elements not found');
      return;
    }

    // Form submission handler
    formEl.addEventListener('submit', handleSubmit);

    // Example command buttons
    const exampleBtns = document.querySelectorAll('.demo__example-btn');
    exampleBtns.forEach(btn => {
      btn.addEventListener('click', () => handleExampleClick(btn));
    });

    // Input field keyboard handler
    inputEl.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        // Form submission is handled by submit event
      }
    });
  }

  /**
   * Handle form submission
   * @param {Event} e - Submit event
   */
  async function handleSubmit(e) {
    e.preventDefault();

    const command = inputEl.value.trim();

    // Handle empty command
    if (!command) {
      addOutput('Please enter a command.', 'error');
      return;
    }

    // Show command in output
    addOutput(command, 'command');

    // Show loading state
    showLoading(true);

    try {
      const result = await executeCommand(command);
      displayOutput(result);
    } catch (error) {
      showError(error.message || 'An error occurred');
    } finally {
      showLoading(false);
      inputEl.value = '';
      inputEl.focus();
    }
  }

  /**
   * Handle example button click
   * @param {HTMLElement} btn - Button element
   */
  function handleExampleClick(btn) {
    const command = btn.dataset.command;
    const value = btn.dataset.value;

    // Set command in input (for SET, append value)
    if (value) {
      inputEl.value = command;
      // For SET commands, we need to handle the value separately
      // Simulate submitting with value
      setTimeout(() => {
        formEl.dispatchEvent(new Event('submit'));
        // After SET, simulate providing the value
        if (command.startsWith('SET')) {
          // The actual SET command with value is handled in executeCommand
        }
      }, 50);
    } else {
      inputEl.value = command;
      formEl.dispatchEvent(new Event('submit'));
    }
  }

  /**
   * Execute a command against the backend or simulation
   * @param {string} command - The Memcached command
   * @returns {Promise<{output: string, success: boolean}>}
   */
  async function executeCommand(command) {
    if (USE_BACKEND) {
      return executeViaBackend(command);
    }
    return executeSimulated(command);
  }

  /**
   * Execute command via backend API
   * @param {string} command - The command to execute
   * @returns {Promise<{output: string, success: boolean}>}
   */
  async function executeViaBackend(command) {
    const response = await fetch(API_ENDPOINT, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ command }),
    });

    if (!response.ok) {
      throw new Error(`Server error: ${response.status}`);
    }

    return response.json();
  }

  /**
   * Execute command using in-memory simulation
   * @param {string} rawCommand - The raw command input
   * @returns {Promise<{output: string, success: boolean}>}
   */
  async function executeSimulated(rawCommand) {
    // Simulate network delay
    await new Promise(resolve => setTimeout(resolve, SIMULATED_DELAY));

    const command = rawCommand.trim();
    const parts = command.split(/\s+/);
    const cmd = parts[0].toUpperCase();

    switch (cmd) {
      case 'SET':
        return handleSet(parts, rawCommand);
      case 'GET':
        return handleGet(parts);
      case 'DELETE':
        return handleDelete(parts);
      case 'STATS':
        return handleStats();
      case 'VERSION':
        return { output: 'VERSION MirDB-Demo 1.0.0', success: true };
      case 'QUIT':
        return { output: 'Connection closed.', success: true };
      default:
        return { output: `ERROR: Unknown command '${cmd}'`, success: false };
    }
  }

  /**
   * Handle SET command
   * SET <key> <flags> <exptime> <bytes> [noreply]\r\n<data>\r\n
   * @param {string[]} parts - Command parts
   * @param {string} rawCommand - Raw command string
   * @returns {{output: string, success: boolean}}
   */
  function handleSet(parts, rawCommand) {
    if (parts.length < 5) {
      return { output: 'CLIENT_ERROR bad command line format', success: false };
    }

    const key = parts[1];
    const flags = parseInt(parts[2], 10);
    const exptime = parseInt(parts[3], 10);
    const bytes = parseInt(parts[4], 10);

    // For demo purposes, generate a sample value or use provided value
    // In real Memcached protocol, value comes on the next line
    // For the demo, we'll accept inline value or use example data
    const exampleBtn = document.querySelector(`[data-command="${rawCommand}"]`);
    let value = exampleBtn?.dataset.value || `data_${key}`;

    if (isNaN(flags) || isNaN(exptime) || isNaN(bytes)) {
      return { output: 'CLIENT_ERROR bad command line format', success: false };
    }

    // Store the value
    demoStore.set(key, {
      value,
      flags,
      exptime,
      bytes: value.length,
      storedAt: Date.now(),
    });

    return { output: 'STORED', success: true };
  }

  /**
   * Handle GET command
   * GET <key>*
   * @param {string[]} parts - Command parts
   * @returns {{output: string, success: boolean}}
   */
  function handleGet(parts) {
    if (parts.length < 2) {
      return { output: 'CLIENT_ERROR missing key', success: false };
    }

    const key = parts[1];
    const item = demoStore.get(key);

    if (!item) {
      return { output: 'END', success: true };
    }

    // Check expiration (if exptime > 0)
    if (item.exptime > 0) {
      const elapsed = (Date.now() - item.storedAt) / 1000;
      if (elapsed > item.exptime) {
        demoStore.delete(key);
        return { output: 'END', success: true };
      }
    }

    // Format: VALUE <key> <flags> <bytes>\r\n<data>\r\nEND
    const output = `VALUE ${key} ${item.flags} ${item.bytes}\r\n${item.value}\r\nEND`;
    return { output, success: true };
  }

  /**
   * Handle DELETE command
   * DELETE <key> [noreply]
   * @param {string[]} parts - Command parts
   * @returns {{output: string, success: boolean}}
   */
  function handleDelete(parts) {
    if (parts.length < 2) {
      return { output: 'CLIENT_ERROR missing key', success: false };
    }

    const key = parts[1];
    const existed = demoStore.has(key);

    if (existed) {
      demoStore.delete(key);
      return { output: 'DELETED', success: true };
    }

    return { output: 'NOT_FOUND', success: true };
  }

  /**
   * Handle STATS command
   * @returns {{output: string, success: boolean}}
   */
  function handleStats() {
    const stats = [
      'STAT pid 1',
      'STAT uptime 300',
      'STAT version MirDB-Demo 1.0.0',
      `STAT curr_items ${demoStore.size}`,
      'STAT total_items ' + demoStore.size,
      'END',
    ];
    return { output: stats.join('\r\n'), success: true };
  }

  /**
   * Display command output in terminal
   * @param {{output: string, success: boolean}} result - Command result
   */
  function displayOutput(result) {
    const className = result.success ? 'response' : 'error';
    addOutput(result.output, className);
  }

  /**
   * Add a line to the output area
   * @param {string} text - Text to display
   * @param {string} type - Line type (command, response, error, info, success)
   */
  function addOutput(text, type = 'response') {
    if (!outputEl) return;

    const line = document.createElement('div');
    line.className = `demo__output-line demo__output-line--${type}`;
    line.textContent = text;

    outputEl.appendChild(line);

    // Auto-scroll to bottom
    outputEl.scrollTop = outputEl.scrollHeight;
  }

  /**
   * Show error message
   * @param {string} message - Error message
   */
  function showError(message) {
    addOutput(`ERROR: ${message}`, 'error');
  }

  /**
   * Show/hide loading state
   * @param {boolean} isLoading - Loading state
   */
  function showLoading(isLoading) {
    if (!submitBtn) return;

    if (isLoading) {
      submitBtn.classList.add('demo__submit--loading');
      submitBtn.disabled = true;
      inputEl.disabled = true;
    } else {
      submitBtn.classList.remove('demo__submit--loading');
      submitBtn.disabled = false;
      inputEl.disabled = false;
    }
  }

  /**
   * Show fallback static example when backend unavailable
   */
  function showFallback() {
    addOutput('Backend unavailable. Using local simulation.', 'info');
  }

  /**
   * Clear the terminal output
   */
  function clearOutput() {
    if (outputEl) {
      outputEl.innerHTML = '';
      addOutput('Terminal cleared.', 'info');
    }
  }

  // Initialize on DOM ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initDemo);
  } else {
    initDemo();
  }

  // Expose for testing and external use
  window.MirDBDemo = {
    initDemo,
    executeCommand,
    displayOutput,
    showError,
    showLoading,
    showFallback,
    clearOutput,
    addOutput,
  };
})();
