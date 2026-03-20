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

  // Error Handling Configuration (Scenario 15)
  const REQUEST_TIMEOUT = 5000; // 5 second timeout for backend requests
  const MAX_COMMANDS_PER_MINUTE = 60; // Rate limit: commands per minute
  const RATE_LIMIT_WINDOW = 60000; // 1 minute window

  // Client-side state tracking
  let commandHistory = []; // Timestamps of recent commands
  let backendAvailable = true; // Track backend availability
  let consecutiveFailures = 0; // Track consecutive backend failures
  const MAX_CONSECUTIVE_FAILURES = 3; // Switch to fallback after this many failures

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

    // Check client-side rate limiting (Scenario 15)
    const rateLimitResult = checkRateLimit();
    if (!rateLimitResult.allowed) {
      addOutput(`Rate limit exceeded. Please wait ${rateLimitResult.waitTime} seconds before sending more commands.`, 'error');
      return;
    }

    // Show command in output
    addOutput(command, 'command');

    // Show loading state
    showLoading(true);

    // Record this command for rate limiting
    recordCommand();

    try {
      const result = await executeCommand(command);
      displayOutput(result);

      // Reset failure counter on success
      if (backendAvailable) {
        consecutiveFailures = 0;
      }
    } catch (error) {
      handleExecutionError(error);
    } finally {
      showLoading(false);
      inputEl.value = '';
      inputEl.focus();
    }
  }

  /**
   * Handle execution errors with graceful degradation (Scenario 15)
   * @param {Error} error - The error that occurred
   */
  function handleExecutionError(error) {
    const message = error.message || 'An unexpected error occurred';

    // Check for specific error types
    if (message.toLowerCase().includes('timeout') || message.toLowerCase().includes('timed out')) {
      showError('Request timed out. The server took too long to respond. Please try again.');
      consecutiveFailures++;
    } else if (message.toLowerCase().includes('rate limit')) {
      showError('Rate limit exceeded. Please wait before sending more commands.');
    } else if (
      message.toLowerCase().includes('network') ||
      message.toLowerCase().includes('unavailable') ||
      message.toLowerCase().includes('failed to fetch') ||
      message.toLowerCase().includes('connection')
    ) {
      consecutiveFailures++;
      handleBackendUnavailable();
    } else {
      showError(message);
      consecutiveFailures++;
    }

    // Switch to fallback mode after multiple failures
    if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES && backendAvailable) {
      switchToFallbackMode();
    }
  }

  /**
   * Handle backend unavailability (Scenario 15)
   */
  function handleBackendUnavailable() {
    addOutput('Backend unavailable. Unable to connect to server.', 'error');
    if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
      switchToFallbackMode();
    }
  }

  /**
   * Switch to fallback simulation mode (Scenario 15)
   */
  function switchToFallbackMode() {
    backendAvailable = false;
    showFallback();
    addOutput('Commands will now use local simulation.', 'info');
  }

  /**
   * Check if request is within rate limits (Scenario 15)
   * @returns {{allowed: boolean, waitTime: number}}
   */
  function checkRateLimit() {
    const now = Date.now();

    // Clean old entries outside the window
    commandHistory = commandHistory.filter(ts => now - ts < RATE_LIMIT_WINDOW);

    // Check if limit exceeded
    if (commandHistory.length >= MAX_COMMANDS_PER_MINUTE) {
      const oldestCommand = Math.min(...commandHistory);
      const waitTime = Math.ceil((RATE_LIMIT_WINDOW - (now - oldestCommand)) / 1000);
      return { allowed: false, waitTime };
    }

    return { allowed: true, waitTime: 0 };
  }

  /**
   * Record a command for rate limiting tracking (Scenario 15)
   */
  function recordCommand() {
    commandHistory.push(Date.now());
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
   * Uses fallback when backend is unavailable (Scenario 15)
   * @param {string} command - The Memcached command
   * @returns {Promise<{output: string, success: boolean}>}
   */
  async function executeCommand(command) {
    // Use simulation if backend is configured off or marked unavailable
    if (!USE_BACKEND || !backendAvailable) {
      return executeSimulated(command);
    }

    // Try backend with fallback to simulation
    try {
      return await executeViaBackend(command);
    } catch (error) {
      // If backend fails, check if we should fall back to simulation
      if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES - 1) {
        // Will switch to fallback after this failure
        throw error;
      }
      // For first few failures, still throw to show error
      throw error;
    }
  }

  /**
   * Execute command via backend API with timeout handling (Scenario 15)
   * @param {string} command - The command to execute
   * @returns {Promise<{output: string, success: boolean}>}
   */
  async function executeViaBackend(command) {
    // Create abort controller for timeout
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);

    try {
      const response = await fetch(API_ENDPOINT, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ command }),
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        // Check for rate limit response from backend
        if (response.status === 429) {
          throw new Error('Rate limit exceeded. Please wait before sending more commands.');
        }
        throw new Error(`Server error: ${response.status}`);
      }

      return response.json();
    } catch (error) {
      clearTimeout(timeoutId);

      // Handle abort (timeout)
      if (error.name === 'AbortError') {
        throw new Error('Request timed out. The server took too long to respond.');
      }

      // Handle network errors
      if (error.message.includes('Failed to fetch') || error.message.includes('NetworkError')) {
        throw new Error('Backend unavailable. Unable to connect to server.');
      }

      throw error;
    }
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
   * Show fallback static example when backend unavailable (Scenario 15)
   * Informs user that demo is using local simulation
   */
  function showFallback() {
    addOutput('Backend unavailable. Using local simulation.', 'info');
    addOutput('You can still try commands - they will work with local data.', 'info');
  }

  /**
   * Reset backend availability status (Scenario 15)
   * Call this to retry connecting to backend
   */
  function resetBackendStatus() {
    backendAvailable = true;
    consecutiveFailures = 0;
    addOutput('Backend status reset. Will try to reconnect on next command.', 'info');
  }

  /**
   * Get current rate limit status (Scenario 15)
   * @returns {{remaining: number, total: number, resetTime: number}}
   */
  function getRateLimitStatus() {
    const now = Date.now();
    commandHistory = commandHistory.filter(ts => now - ts < RATE_LIMIT_WINDOW);
    const remaining = Math.max(0, MAX_COMMANDS_PER_MINUTE - commandHistory.length);
    const oldestCommand = commandHistory.length > 0 ? Math.min(...commandHistory) : now;
    const resetTime = Math.max(0, Math.ceil((RATE_LIMIT_WINDOW - (now - oldestCommand)) / 1000));

    return {
      remaining,
      total: MAX_COMMANDS_PER_MINUTE,
      resetTime: remaining === MAX_COMMANDS_PER_MINUTE ? 0 : resetTime,
    };
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
    // Scenario 15: Error handling and graceful degradation
    checkRateLimit,
    getRateLimitStatus,
    resetBackendStatus,
    recordCommand,
  };
})();
