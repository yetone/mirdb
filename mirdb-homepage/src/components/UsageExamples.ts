/**
 * Usage Examples Section Component.
 * Owner: Scenario 3 - Usage Examples with Syntax Highlighting
 *
 * Displays terminal-style code blocks showing memcached commands:
 * - set command example
 * - get command example
 * - delete command example
 *
 * Uses syntax-highlighter utility for code highlighting.
 *
 * Expected exports:
 * - renderUsageExamples(): HTMLElement
 * - CODE_EXAMPLES: CodeExample[]
 */

import type { CodeExample } from '../types/index';
import { createCodeBlock } from '../utils/syntax-highlighter';

export const CODE_EXAMPLES: CodeExample[] = [
  {
    command: 'set key 0 0 5\r\nvalue\r\n',
    description: 'Store a key-value pair with no expiration',
    response: 'STORED',
  },
  {
    command: 'get key',
    description: 'Retrieve the value for a key',
    response: 'VALUE key 0 5\r\nvalue\r\nEND',
  },
  {
    command: 'delete key',
    description: 'Remove a key from the database',
    response: 'DELETED',
  },
];

/**
 * Creates a single example card with command and response.
 */
function createExampleCard(example: CodeExample, index: number): HTMLElement {
  const card = document.createElement('div');
  card.className = 'usage-example-card';
  card.setAttribute('data-example-id', String(index + 1));

  // Description
  const description = document.createElement('p');
  description.className = 'usage-example-description';
  description.textContent = example.description;
  card.appendChild(description);

  // Command code block
  const commandLabel = document.createElement('span');
  commandLabel.className = 'usage-example-label';
  commandLabel.textContent = 'Command:';
  card.appendChild(commandLabel);

  const commandBlock = createCodeBlock(example.command, 'memcached');
  commandBlock.classList.add('usage-command-block');
  card.appendChild(commandBlock);

  // Response code block (if present)
  if (example.response) {
    const responseLabel = document.createElement('span');
    responseLabel.className = 'usage-example-label';
    responseLabel.textContent = 'Response:';
    card.appendChild(responseLabel);

    const responseBlock = createCodeBlock(example.response, 'memcached');
    responseBlock.classList.add('usage-response-block');
    card.appendChild(responseBlock);
  }

  return card;
}

/**
 * Renders the complete Usage Examples section.
 */
export function renderUsageExamples(): HTMLElement {
  const section = document.createElement('section');
  section.id = 'usage';
  section.className = 'usage-section';
  section.setAttribute('aria-labelledby', 'usage-title');

  // Section title
  const title = document.createElement('h2');
  title.id = 'usage-title';
  title.className = 'usage-title';
  title.textContent = 'Usage Examples';
  section.appendChild(title);

  // Section description
  const intro = document.createElement('p');
  intro.className = 'usage-intro';
  intro.textContent = 'MirDB uses the memcached text protocol. Here are some common commands:';
  section.appendChild(intro);

  // Code examples container
  const container = document.createElement('div');
  container.className = 'usage-examples-container';

  for (let i = 0; i < CODE_EXAMPLES.length; i++) {
    const card = createExampleCard(CODE_EXAMPLES[i], i);
    container.appendChild(card);
  }

  section.appendChild(container);

  return section;
}
