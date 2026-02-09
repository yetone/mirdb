/**
 * Configuration Reference Section Component.
 * Owner: Scenario 6 - Configuration Reference
 *
 * Requirements: REQ-6
 *
 * Renders a table of configuration options showing name, default value,
 * and description for each configurable setting from mirdb.toml.
 */

import { configOptions } from '../data/config';
import type { ConfigOption } from '../types';

function createConfigRow(option: ConfigOption): HTMLTableRowElement {
  const row = document.createElement('tr');
  row.className = 'config-row';

  const nameCell = document.createElement('td');
  nameCell.className = 'config-name';
  const nameCode = document.createElement('code');
  nameCode.textContent = option.name;
  nameCell.appendChild(nameCode);

  const defaultCell = document.createElement('td');
  defaultCell.className = 'config-default';
  const defaultCode = document.createElement('code');
  defaultCode.textContent = option.default;
  defaultCell.appendChild(defaultCode);

  const descCell = document.createElement('td');
  descCell.className = 'config-description';
  descCell.textContent = option.description;

  row.appendChild(nameCell);
  row.appendChild(defaultCell);
  row.appendChild(descCell);

  return row;
}

export function renderConfiguration(): HTMLElement {
  const section = document.createElement('section');
  section.id = 'configuration';
  section.className = 'configuration-section';
  section.setAttribute('aria-labelledby', 'configuration-heading');

  const container = document.createElement('div');
  container.className = 'container';

  const heading = document.createElement('h2');
  heading.id = 'configuration-heading';
  heading.textContent = 'Configuration Reference';

  const intro = document.createElement('p');
  intro.className = 'configuration-intro';
  intro.textContent = 'MirDB can be configured using a TOML file (mirdb.toml). Below are all available configuration options with their default values.';

  const tableWrapper = document.createElement('div');
  tableWrapper.className = 'config-table-wrapper';

  const table = document.createElement('table');
  table.className = 'config-table';
  table.setAttribute('role', 'table');
  table.setAttribute('aria-label', 'Configuration options');

  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');

  const headers = ['Option', 'Default', 'Description'];
  headers.forEach((headerText) => {
    const th = document.createElement('th');
    th.scope = 'col';
    th.textContent = headerText;
    headerRow.appendChild(th);
  });

  thead.appendChild(headerRow);
  table.appendChild(thead);

  const tbody = document.createElement('tbody');
  configOptions.forEach((option) => {
    const row = createConfigRow(option);
    tbody.appendChild(row);
  });

  table.appendChild(tbody);
  tableWrapper.appendChild(table);

  container.appendChild(heading);
  container.appendChild(intro);
  container.appendChild(tableWrapper);
  section.appendChild(container);

  return section;
}
