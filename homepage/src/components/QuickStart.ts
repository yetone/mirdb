/**
 * Quick Start Section Component.
 * Owner: Scenario 3 - Quick Start Section
 *
 * Requirements: REQ-3, REQ-11
 *
 * Provides installation instructions, server startup commands,
 * and example Memcached commands with copy-to-clipboard functionality.
 */

import { attachCopyButton } from '../utils/clipboard';
import type { CodeBlock } from '../types';

const codeBlocks: CodeBlock[] = [
  {
    code: 'cargo install mirdb',
    language: 'bash',
    label: 'Installation',
  },
  {
    code: 'mirdb',
    language: 'bash',
    label: 'Start Server',
  },
  {
    code: `# Connect using netcat or telnet
nc localhost 12333

# Store a value
set mykey 0 0 5
hello
STORED

# Retrieve the value
get mykey
VALUE mykey 0 5
hello
END

# Delete the value
delete mykey
DELETED`,
    language: 'bash',
    label: 'Example Commands',
  },
];

/**
 * Applies basic syntax highlighting to code.
 * @param code - The code string to highlight
 * @param language - The language for highlighting
 * @returns HTML string with syntax highlighting spans
 */
function highlightCode(code: string, language: string): string {
  // Simple highlighting for bash/shell commands
  if (language === 'bash' || language === 'shell') {
    return code
      // Highlight comments
      .replace(/(#.*$)/gm, '<span class="token comment">$1</span>')
      // Highlight response keywords
      .replace(/\b(STORED|DELETED|END|VALUE|ERROR|NOT_FOUND)\b/g, '<span class="token keyword">$1</span>')
      // Highlight commands
      .replace(/^(set|get|delete|add|replace|append|prepend|cargo|mirdb|nc)\b/gm, '<span class="token function">$1</span>');
  }
  return code;
}

/**
 * Creates a code block element with syntax highlighting and copy button.
 * @param block - The code block configuration
 * @returns HTMLElement containing the styled code block
 */
function createCodeBlock(block: CodeBlock): HTMLElement {
  const container = document.createElement('div');
  container.className = 'quickstart-code-block';

  if (block.label) {
    const label = document.createElement('div');
    label.className = 'code-label';
    label.textContent = block.label;
    container.appendChild(label);
  }

  const pre = document.createElement('pre');
  pre.className = `language-${block.language}`;
  pre.setAttribute('data-language', block.language);

  const code = document.createElement('code');
  code.className = `language-${block.language}`;
  code.innerHTML = highlightCode(block.code, block.language);

  pre.appendChild(code);
  container.appendChild(pre);

  // Attach copy button
  attachCopyButton(pre);

  return container;
}

/**
 * Renders the Quick Start section.
 * @returns HTMLElement containing the complete Quick Start section
 */
export function renderQuickStart(): HTMLElement {
  const section = document.createElement('section');
  section.id = 'quickstart';
  section.className = 'quickstart-section';
  section.setAttribute('aria-labelledby', 'quickstart-title');

  const container = document.createElement('div');
  container.className = 'container';

  const title = document.createElement('h2');
  title.id = 'quickstart-title';
  title.textContent = 'Quick Start';

  const intro = document.createElement('p');
  intro.className = 'quickstart-intro';
  intro.textContent = 'Get up and running with MirDB in just a few steps. MirDB listens on port 12333 by default.';

  container.appendChild(title);
  container.appendChild(intro);

  // Create step-by-step instructions
  const steps = [
    { title: '1. Install MirDB', description: 'Install using Cargo, the Rust package manager:', block: codeBlocks[0] },
    { title: '2. Start the Server', description: 'Run the MirDB server:', block: codeBlocks[1] },
    { title: '3. Connect and Use', description: 'Connect using any Memcached-compatible client or netcat:', block: codeBlocks[2] },
  ];

  const stepsContainer = document.createElement('div');
  stepsContainer.className = 'quickstart-steps';

  steps.forEach(step => {
    const stepEl = document.createElement('div');
    stepEl.className = 'quickstart-step';

    const stepTitle = document.createElement('h3');
    stepTitle.textContent = step.title;

    const stepDesc = document.createElement('p');
    stepDesc.textContent = step.description;

    stepEl.appendChild(stepTitle);
    stepEl.appendChild(stepDesc);
    stepEl.appendChild(createCodeBlock(step.block));

    stepsContainer.appendChild(stepEl);
  });

  container.appendChild(stepsContainer);
  section.appendChild(container);

  return section;
}
