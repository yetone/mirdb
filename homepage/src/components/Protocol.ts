/**
 * Protocol Documentation Section Component.
 * Owner: Scenario 5 - Protocol Documentation
 *
 * Requirements: REQ-5
 *
 * Renders documentation for all supported Memcached commands,
 * with special highlighting for MirDB-specific commands.
 */

import { commands, ProtocolCommand } from '../data/commands';

function createCommandCard(command: ProtocolCommand): HTMLElement {
  const card = document.createElement('article');
  card.className = 'command-card';
  if (command.isMirDBSpecific) {
    card.classList.add('mirdb-specific');
  }

  const header = document.createElement('div');
  header.className = 'command-header';

  const name = document.createElement('h3');
  name.className = 'command-name';
  name.textContent = command.name;

  header.appendChild(name);

  if (command.isMirDBSpecific) {
    const badge = document.createElement('span');
    badge.className = 'mirdb-badge';
    badge.textContent = 'MirDB';
    badge.setAttribute('aria-label', 'MirDB-specific command');
    header.appendChild(badge);
  }

  const description = document.createElement('p');
  description.className = 'command-description';
  description.textContent = command.description;

  const syntaxContainer = document.createElement('div');
  syntaxContainer.className = 'command-syntax-container';

  const syntaxLabel = document.createElement('span');
  syntaxLabel.className = 'command-label';
  syntaxLabel.textContent = 'Syntax:';

  const syntaxCode = document.createElement('code');
  syntaxCode.className = 'command-syntax';
  syntaxCode.textContent = command.syntax;

  syntaxContainer.appendChild(syntaxLabel);
  syntaxContainer.appendChild(syntaxCode);

  card.appendChild(header);
  card.appendChild(description);
  card.appendChild(syntaxContainer);

  if (command.example) {
    const exampleContainer = document.createElement('div');
    exampleContainer.className = 'command-example-container';

    const exampleLabel = document.createElement('span');
    exampleLabel.className = 'command-label';
    exampleLabel.textContent = 'Example:';

    const exampleCode = document.createElement('code');
    exampleCode.className = 'command-example';
    exampleCode.textContent = command.example;

    exampleContainer.appendChild(exampleLabel);
    exampleContainer.appendChild(exampleCode);
    card.appendChild(exampleContainer);
  }

  return card;
}

export function renderProtocol(): HTMLElement {
  const section = document.createElement('section');
  section.id = 'protocol';
  section.className = 'protocol-section';
  section.setAttribute('aria-labelledby', 'protocol-heading');

  const container = document.createElement('div');
  container.className = 'container';

  const heading = document.createElement('h2');
  heading.id = 'protocol-heading';
  heading.textContent = 'Protocol Documentation';

  const intro = document.createElement('p');
  intro.className = 'protocol-intro';
  intro.textContent = 'MirDB implements the Memcached text protocol, enabling compatibility with existing Memcached clients. Below are the supported commands.';

  const commandsGrid = document.createElement('div');
  commandsGrid.className = 'commands-grid';

  commands.forEach((command) => {
    const card = createCommandCard(command);
    commandsGrid.appendChild(card);
  });

  container.appendChild(heading);
  container.appendChild(intro);
  container.appendChild(commandsGrid);
  section.appendChild(container);

  return section;
}
