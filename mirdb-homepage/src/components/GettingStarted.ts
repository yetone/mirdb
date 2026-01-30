/**
 * Getting Started Section Component.
 * Owner: Scenario 5 - Getting Started Section
 *
 * Provides:
 * - Installation commands (cargo install, build from source)
 * - Quick start example with minimal configuration
 * - Default configuration values (port 12333, /tmp/mirdb)
 * - Link to full documentation
 *
 * Expected exports:
 * - renderGettingStarted(): HTMLElement
 * - INSTALL_COMMANDS: string[]
 */

export interface InstallOption {
  id: string;
  title: string;
  commands: string[];
  description: string;
}

/**
 * Installation commands for MirDB
 */
export const INSTALL_COMMANDS: string[] = [
  'cargo install mirdb',
  'git clone https://github.com/pjzhong/mirdb.git',
  'cargo build --release',
];

/**
 * Installation options with detailed commands
 */
export const INSTALL_OPTIONS: InstallOption[] = [
  {
    id: 'cargo',
    title: 'Using Cargo',
    commands: ['cargo install mirdb'],
    description: 'Install directly from crates.io (recommended)',
  },
  {
    id: 'source',
    title: 'Build from Source',
    commands: [
      'git clone https://github.com/pjzhong/mirdb.git',
      'cd mirdb',
      'cargo build --release',
    ],
    description: 'Clone the repository and build from source',
  },
];

/**
 * Default configuration values
 */
export const DEFAULT_CONFIG = {
  port: 12333,
  workDir: '/tmp/mirdb',
};

/**
 * Quick start configuration example
 */
export const QUICK_START_CONFIG = `# Start MirDB with default settings
mirdb

# Or specify custom port and directory
mirdb --port ${DEFAULT_CONFIG.port} --work-dir ${DEFAULT_CONFIG.workDir}`;

/**
 * Creates a code block element with copy functionality
 */
function createCodeBlock(code: string, id: string): HTMLElement {
  const container = document.createElement('div');
  container.className = 'code-block-container relative bg-gray-900 rounded-lg overflow-hidden';
  container.setAttribute('data-testid', `code-block-${id}`);

  const pre = document.createElement('pre');
  pre.className = 'code-block p-4 overflow-x-auto text-sm';
  pre.setAttribute('data-testid', 'code-block');

  const code_elem = document.createElement('code');
  code_elem.className = 'code-content text-green-400 font-mono whitespace-pre';
  code_elem.setAttribute('data-testid', 'code-content');
  code_elem.textContent = code;

  const copyButton = document.createElement('button');
  copyButton.className = 'copy-button absolute top-2 right-2 px-3 py-1 bg-gray-700 hover:bg-gray-600 text-gray-300 rounded text-xs transition-colors';
  copyButton.setAttribute('data-testid', 'copy-button');
  copyButton.setAttribute('aria-label', 'Copy to clipboard');
  copyButton.textContent = 'Copy';
  copyButton.addEventListener('click', () => {
    navigator.clipboard.writeText(code).then(() => {
      copyButton.textContent = 'Copied!';
      setTimeout(() => {
        copyButton.textContent = 'Copy';
      }, 2000);
    });
  });

  pre.appendChild(code_elem);
  container.appendChild(pre);
  container.appendChild(copyButton);

  return container;
}

/**
 * Creates an installation option card
 */
function createInstallOption(option: InstallOption): HTMLElement {
  const card = document.createElement('div');
  card.className = 'install-option bg-white rounded-lg shadow-md p-6';
  card.setAttribute('data-testid', `install-option-${option.id}`);

  const title = document.createElement('h3');
  title.className = 'install-option-title text-xl font-semibold text-gray-900 mb-2';
  title.setAttribute('data-testid', 'install-option-title');
  title.textContent = option.title;

  const description = document.createElement('p');
  description.className = 'install-option-description text-gray-600 mb-4';
  description.setAttribute('data-testid', 'install-option-description');
  description.textContent = option.description;

  card.appendChild(title);
  card.appendChild(description);

  const commandsCode = option.commands.join('\n');
  const codeBlock = createCodeBlock(commandsCode, option.id);
  card.appendChild(codeBlock);

  return card;
}

/**
 * Creates the default configuration section
 */
function createDefaultConfigSection(): HTMLElement {
  const section = document.createElement('div');
  section.className = 'default-config mt-12';
  section.setAttribute('data-testid', 'default-config');

  const title = document.createElement('h3');
  title.className = 'text-xl font-semibold text-gray-900 mb-4';
  title.textContent = 'Default Configuration';

  const configGrid = document.createElement('div');
  configGrid.className = 'config-grid grid grid-cols-1 md:grid-cols-2 gap-4 mb-6';
  configGrid.setAttribute('data-testid', 'config-grid');

  const portConfig = document.createElement('div');
  portConfig.className = 'config-item bg-gray-100 rounded-lg p-4';
  portConfig.setAttribute('data-testid', 'config-port');
  portConfig.innerHTML = `
    <span class="config-label text-gray-600 text-sm">Default Port</span>
    <p class="config-value text-2xl font-mono font-bold text-blue-600">${DEFAULT_CONFIG.port}</p>
  `;

  const workDirConfig = document.createElement('div');
  workDirConfig.className = 'config-item bg-gray-100 rounded-lg p-4';
  workDirConfig.setAttribute('data-testid', 'config-workdir');
  workDirConfig.innerHTML = `
    <span class="config-label text-gray-600 text-sm">Work Directory</span>
    <p class="config-value text-2xl font-mono font-bold text-blue-600">${DEFAULT_CONFIG.workDir}</p>
  `;

  configGrid.appendChild(portConfig);
  configGrid.appendChild(workDirConfig);

  section.appendChild(title);
  section.appendChild(configGrid);

  return section;
}

/**
 * Creates the quick start section
 */
function createQuickStartSection(): HTMLElement {
  const section = document.createElement('div');
  section.className = 'quick-start mt-12';
  section.setAttribute('data-testid', 'quick-start');

  const title = document.createElement('h3');
  title.className = 'text-xl font-semibold text-gray-900 mb-4';
  title.textContent = 'Quick Start';

  const description = document.createElement('p');
  description.className = 'text-gray-600 mb-4';
  description.textContent = 'Start MirDB with the following command:';

  const codeBlock = createCodeBlock(QUICK_START_CONFIG, 'quick-start');

  section.appendChild(title);
  section.appendChild(description);
  section.appendChild(codeBlock);

  return section;
}

/**
 * Creates the documentation link section
 */
function createDocumentationLink(): HTMLElement {
  const container = document.createElement('div');
  container.className = 'documentation-link mt-12 text-center';
  container.setAttribute('data-testid', 'documentation-link');

  const link = document.createElement('a');
  link.className = 'doc-link inline-flex items-center px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg transition-colors';
  link.href = 'https://github.com/pjzhong/mirdb#readme';
  link.target = '_blank';
  link.rel = 'noopener noreferrer';
  link.setAttribute('data-testid', 'doc-link');
  link.innerHTML = `
    <span>View Full Documentation</span>
    <svg xmlns="http://www.w3.org/2000/svg" class="w-5 h-5 ml-2" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
    </svg>
  `;

  container.appendChild(link);

  return container;
}

/**
 * Renders the Getting Started section
 * @returns HTMLElement - The Getting Started section element
 */
export function renderGettingStarted(): HTMLElement {
  const section = document.createElement('section');
  section.className = 'getting-started-section py-16 px-4 bg-white';
  section.id = 'getting-started';
  section.setAttribute('data-testid', 'getting-started-section');

  const container = document.createElement('div');
  container.className = 'getting-started-container max-w-4xl mx-auto';

  const sectionTitle = document.createElement('h2');
  sectionTitle.className = 'getting-started-title text-3xl font-bold text-center text-gray-900 mb-4';
  sectionTitle.textContent = 'Getting Started';

  const sectionDescription = document.createElement('p');
  sectionDescription.className = 'getting-started-description text-lg text-center text-gray-600 mb-12';
  sectionDescription.textContent = 'Get MirDB up and running in minutes with these simple installation options.';

  const installGrid = document.createElement('div');
  installGrid.className = 'install-grid grid grid-cols-1 md:grid-cols-2 gap-6';
  installGrid.setAttribute('data-testid', 'install-grid');

  // Add installation option cards
  INSTALL_OPTIONS.forEach((option) => {
    const card = createInstallOption(option);
    installGrid.appendChild(card);
  });

  container.appendChild(sectionTitle);
  container.appendChild(sectionDescription);
  container.appendChild(installGrid);
  container.appendChild(createDefaultConfigSection());
  container.appendChild(createQuickStartSection());
  container.appendChild(createDocumentationLink());
  section.appendChild(container);

  return section;
}
