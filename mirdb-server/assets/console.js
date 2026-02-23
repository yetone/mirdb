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

// Initialize console when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    console.log('MirDB Console initialized');
});
