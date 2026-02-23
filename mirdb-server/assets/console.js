/**
 * MirDB Interactive Console JavaScript
 * Owner: Scenario 4 - Interactive Console SET Command
 */

// Console API endpoint
const CONSOLE_API = '/api/console';

/**
 * Submit a command to the console API
 * @param {string} command - The memcached command to execute
 * @returns {Promise<string>} - The command response
 */
async function submitCommand(command) {
    try {
        const response = await fetch(CONSOLE_API, {
            method: 'POST',
            headers: {
                'Content-Type': 'text/plain',
            },
            body: command,
        });

        if (!response.ok) {
            throw new Error(`HTTP error: ${response.status}`);
        }

        const data = await response.json();
        return data.message || data.response || JSON.stringify(data);
    } catch (error) {
        return `Error: ${error.message}`;
    }
}

/**
 * Display a response in the console output area
 * @param {string} response - The response text to display
 */
function displayResponse(response) {
    const output = document.getElementById('console-output');
    if (output) {
        const line = document.createElement('div');
        line.textContent = response;
        output.appendChild(line);
        output.scrollTop = output.scrollHeight;
    }
}

// ============================================================================
// Terminal Demo Animation (Scenario 7)
// ============================================================================

/**
 * Terminal demo animation configuration
 * Shows a realistic sequence of memcached commands
 */
const TERMINAL_ANIMATION = {
    // Commands to demonstrate (with responses)
    commands: [
        {
            prompt: '$ telnet localhost 11211',
            response: 'Trying 127.0.0.1...\nConnected to localhost.',
            delay: 1500
        },
        {
            prompt: 'set user:1 0 0 4',
            response: null,  // waiting for value
            delay: 800
        },
        {
            prompt: 'john',
            response: 'STORED',
            delay: 600
        },
        {
            prompt: 'get user:1',
            response: 'VALUE user:1 0 4\njohn\nEND',
            delay: 1200
        },
        {
            prompt: 'set counter 0 0 1',
            response: null,
            delay: 800
        },
        {
            prompt: '5',
            response: 'STORED',
            delay: 600
        },
        {
            prompt: 'get counter',
            response: 'VALUE counter 0 1\n5\nEND',
            delay: 1200
        },
        {
            prompt: 'delete user:1',
            response: 'DELETED',
            delay: 1000
        },
        {
            prompt: 'get user:1',
            response: 'END',
            delay: 1200
        }
    ],
    typingSpeed: 50,       // ms per character
    pauseBetweenLoops: 3000 // ms before restarting animation
};

/**
 * Terminal animation state
 */
let animationState = {
    isRunning: false,
    currentCommandIndex: 0,
    abortController: null
};

/**
 * Sleep utility for animation timing
 * @param {number} ms - Milliseconds to sleep
 * @returns {Promise<void>}
 */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Type text character by character into a container
 * @param {HTMLElement} container - The container element to type into
 * @param {string} text - The text to type
 * @param {number} speed - Typing speed in ms per character
 * @param {string} [className] - Optional class name for the span
 * @returns {Promise<HTMLElement>} - The created span element
 */
async function typeText(container, text, speed, className = '') {
    const span = document.createElement('span');
    if (className) {
        span.className = className;
    }
    container.appendChild(span);

    for (let i = 0; i < text.length; i++) {
        span.textContent += text[i];
        await sleep(speed);
    }

    return span;
}

/**
 * Add a line to the terminal output
 * @param {HTMLElement} terminal - The terminal element
 * @param {string} text - The text to add
 * @param {string} [className] - Optional class name for the line
 * @returns {HTMLElement} - The created line element
 */
function addLine(terminal, text, className = '') {
    const line = document.createElement('div');
    line.className = 'terminal-line' + (className ? ' ' + className : '');
    line.textContent = text;
    terminal.appendChild(line);
    scrollTerminal(terminal);
    return line;
}

/**
 * Scroll terminal to bottom
 * @param {HTMLElement} terminal - The terminal element
 */
function scrollTerminal(terminal) {
    const terminalBody = terminal.closest('.terminal-body') || terminal;
    terminalBody.scrollTop = terminalBody.scrollHeight;
}

/**
 * Clear terminal content
 * @param {HTMLElement} terminal - The terminal element
 */
function clearTerminal(terminal) {
    terminal.innerHTML = '';
}

/**
 * Run a single command animation
 * @param {HTMLElement} terminal - The terminal output element
 * @param {object} command - The command object with prompt and response
 * @returns {Promise<void>}
 */
async function animateCommand(terminal, command) {
    // Create line for prompt
    const promptLine = document.createElement('div');
    promptLine.className = 'terminal-line terminal-prompt';
    terminal.appendChild(promptLine);

    // Type the prompt/command
    await typeText(promptLine, command.prompt, TERMINAL_ANIMATION.typingSpeed);

    // Pause after typing
    await sleep(300);

    // Show response if there is one
    if (command.response) {
        const responseLines = command.response.split('\n');
        for (const line of responseLines) {
            addLine(terminal, line, 'terminal-response');
        }
    }

    // Wait before next command
    await sleep(command.delay);
}

/**
 * Run the terminal demo animation loop
 * @returns {Promise<void>}
 */
async function runTerminalAnimation() {
    const terminalOutput = document.getElementById('terminal-animation-output');
    if (!terminalOutput) {
        console.warn('Terminal animation output element not found');
        return;
    }

    animationState.isRunning = true;

    while (animationState.isRunning) {
        // Clear terminal at start of each loop
        clearTerminal(terminalOutput);

        // Run through all commands
        for (const command of TERMINAL_ANIMATION.commands) {
            if (!animationState.isRunning) break;
            await animateCommand(terminalOutput, command);
        }

        if (!animationState.isRunning) break;

        // Pause before restarting the loop
        await sleep(TERMINAL_ANIMATION.pauseBetweenLoops);
    }
}

/**
 * Initialize the terminal demo animation
 * Called when the page loads
 */
function initTerminalAnimation() {
    const terminalDemo = document.querySelector('.terminal-demo');
    if (!terminalDemo) {
        console.warn('Terminal demo container not found');
        return;
    }

    // Check if animation output already exists
    let terminalOutput = document.getElementById('terminal-animation-output');
    if (!terminalOutput) {
        // Find the terminal body and set up animation container
        const terminalBody = terminalDemo.querySelector('.terminal-body');
        if (terminalBody) {
            // Clear any static content
            terminalBody.innerHTML = '';

            // Create animation output container
            terminalOutput = document.createElement('div');
            terminalOutput.id = 'terminal-animation-output';
            terminalOutput.className = 'terminal-animation-output';
            terminalOutput.setAttribute('aria-live', 'polite');
            terminalOutput.setAttribute('aria-label', 'Terminal demo animation showing MirDB commands');
            terminalBody.appendChild(terminalOutput);
        }
    }

    // Start the animation
    runTerminalAnimation();
}

/**
 * Stop the terminal animation
 */
function stopTerminalAnimation() {
    animationState.isRunning = false;
}

// ============================================================================
// Navigation and Smooth Scroll (Scenario 15)
// ============================================================================

/**
 * Handle navigation anchor click with smooth scroll
 * @param {Event} event - The click event
 * @param {string} targetId - The target section ID (without #)
 */
function handleNavigation(event, targetId) {
    const targetElement = document.getElementById(targetId);
    if (targetElement) {
        event.preventDefault();
        targetElement.scrollIntoView({
            behavior: 'smooth',
            block: 'start'
        });
        // Update URL hash without triggering jump
        history.pushState(null, '', '#' + targetId);
        // Set focus to the target section for accessibility
        targetElement.setAttribute('tabindex', '-1');
        targetElement.focus({ preventScroll: true });
    }
}

/**
 * Initialize smooth scroll for all internal navigation links
 * Sets up event listeners for anchor links pointing to page sections
 */
function initSmoothScroll() {
    // Select all anchor links that point to internal sections
    const navLinks = document.querySelectorAll('a[href^="#"]');

    navLinks.forEach(link => {
        link.addEventListener('click', function(event) {
            const href = this.getAttribute('href');
            // Skip if it's just "#" or empty
            if (!href || href === '#') return;

            const targetId = href.substring(1); // Remove the #
            handleNavigation(event, targetId);
        });
    });
}

/**
 * Handle initial page load with hash in URL
 * Smoothly scrolls to section if hash is present
 */
function handleInitialHash() {
    if (window.location.hash) {
        const targetId = window.location.hash.substring(1);
        const targetElement = document.getElementById(targetId);
        if (targetElement) {
            // Small delay to ensure page is fully loaded
            setTimeout(() => {
                targetElement.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
            }, 100);
        }
    }
}

// Initialize console when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    console.log('MirDB Console initialized');

    // Initialize terminal demo animation
    initTerminalAnimation();

    // Initialize smooth scroll navigation (Scenario 15)
    initSmoothScroll();

    // Handle initial hash in URL
    handleInitialHash();
});
