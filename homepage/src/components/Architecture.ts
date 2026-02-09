/**
 * Architecture Section Component.
 * Owner: Scenario 4 - Architecture Visualization
 *
 * Requirements: REQ-4
 *
 * Renders an LSM tree data flow diagram showing:
 * - WAL (Write-Ahead Log)
 * - Memtable
 * - Immutable Memtables
 * - SSTable levels (L0, L1, L2...)
 * - Data flow arrows between components
 */

interface DiagramComponent {
  id: string;
  label: string;
  sublabel?: string;
  className: string;
}

const components: DiagramComponent[] = [
  { id: 'wal', label: 'WAL', sublabel: 'Write-Ahead Log', className: 'arch-wal' },
  { id: 'memtable', label: 'Memtable', sublabel: 'Active (Skip List)', className: 'arch-memtable' },
  { id: 'immutable', label: 'Immutable Memtables', sublabel: 'Frozen', className: 'arch-immutable' },
  { id: 'sstable-l0', label: 'L0', sublabel: 'SSTable Level 0', className: 'arch-sstable arch-l0' },
  { id: 'sstable-l1', label: 'L1', sublabel: 'SSTable Level 1', className: 'arch-sstable arch-l1' },
  { id: 'sstable-l2', label: 'L2', sublabel: 'SSTable Level 2', className: 'arch-sstable arch-l2' },
];

function createDiagramNode(component: DiagramComponent): HTMLElement {
  const node = document.createElement('div');
  node.className = `arch-node ${component.className}`;
  node.setAttribute('data-component', component.id);
  node.setAttribute('role', 'img');
  node.setAttribute('aria-label', `${component.label}: ${component.sublabel || ''}`);

  const label = document.createElement('span');
  label.className = 'arch-label';
  label.textContent = component.label;

  const sublabel = document.createElement('span');
  sublabel.className = 'arch-sublabel';
  sublabel.textContent = component.sublabel || '';

  node.appendChild(label);
  node.appendChild(sublabel);

  return node;
}

function createArrow(from: string, to: string, label?: string): HTMLElement {
  const arrow = document.createElement('div');
  arrow.className = `arch-arrow arch-arrow-${from}-${to}`;
  arrow.setAttribute('role', 'img');
  arrow.setAttribute('aria-label', `Data flows from ${from} to ${to}${label ? `: ${label}` : ''}`);

  const arrowLine = document.createElement('div');
  arrowLine.className = 'arch-arrow-line';

  const arrowHead = document.createElement('div');
  arrowHead.className = 'arch-arrow-head';

  if (label) {
    const arrowLabel = document.createElement('span');
    arrowLabel.className = 'arch-arrow-label';
    arrowLabel.textContent = label;
    arrow.appendChild(arrowLabel);
  }

  arrow.appendChild(arrowLine);
  arrow.appendChild(arrowHead);

  return arrow;
}

function createWritePath(): HTMLElement {
  const writePath = document.createElement('div');
  writePath.className = 'arch-write-path';

  // Write entry point
  const writeEntry = document.createElement('div');
  writeEntry.className = 'arch-write-entry';
  writeEntry.textContent = 'Write';
  writeEntry.setAttribute('role', 'img');
  writeEntry.setAttribute('aria-label', 'Write operation entry point');

  writePath.appendChild(writeEntry);
  writePath.appendChild(createArrow('write', 'wal'));

  return writePath;
}

function createMemorySection(): HTMLElement {
  const memorySection = document.createElement('div');
  memorySection.className = 'arch-memory-section';

  const memoryLabel = document.createElement('div');
  memoryLabel.className = 'arch-section-label';
  memoryLabel.textContent = 'Memory';

  // WAL and Memtable row
  const walMemtableRow = document.createElement('div');
  walMemtableRow.className = 'arch-row arch-wal-memtable-row';

  const walNode = createDiagramNode(components[0]); // WAL
  const memtableNode = createDiagramNode(components[1]); // Memtable

  walMemtableRow.appendChild(walNode);
  walMemtableRow.appendChild(createArrow('wal', 'memtable'));
  walMemtableRow.appendChild(memtableNode);

  // Immutable Memtables row
  const immutableRow = document.createElement('div');
  immutableRow.className = 'arch-row arch-immutable-row';

  const immutableNode = createDiagramNode(components[2]); // Immutable
  immutableRow.appendChild(immutableNode);

  memorySection.appendChild(memoryLabel);
  memorySection.appendChild(walMemtableRow);
  memorySection.appendChild(createArrow('memtable', 'immutable', 'Flush'));
  memorySection.appendChild(immutableRow);

  return memorySection;
}

function createDiskSection(): HTMLElement {
  const diskSection = document.createElement('div');
  diskSection.className = 'arch-disk-section';

  const diskLabel = document.createElement('div');
  diskLabel.className = 'arch-section-label';
  diskLabel.textContent = 'Disk (SSTables)';

  // SSTable levels
  const sstableContainer = document.createElement('div');
  sstableContainer.className = 'arch-sstable-container';

  // L0
  const l0Row = document.createElement('div');
  l0Row.className = 'arch-row arch-l0-row';
  l0Row.appendChild(createDiagramNode(components[3])); // L0

  // L1
  const l1Row = document.createElement('div');
  l1Row.className = 'arch-row arch-l1-row';
  l1Row.appendChild(createDiagramNode(components[4])); // L1

  // L2
  const l2Row = document.createElement('div');
  l2Row.className = 'arch-row arch-l2-row';
  l2Row.appendChild(createDiagramNode(components[5])); // L2

  sstableContainer.appendChild(l0Row);
  sstableContainer.appendChild(createArrow('l0', 'l1', 'Minor Compaction'));
  sstableContainer.appendChild(l1Row);
  sstableContainer.appendChild(createArrow('l1', 'l2', 'Major Compaction'));
  sstableContainer.appendChild(l2Row);

  diskSection.appendChild(diskLabel);
  diskSection.appendChild(sstableContainer);

  return diskSection;
}

function createLSMDiagram(): HTMLElement {
  const diagram = document.createElement('div');
  diagram.className = 'arch-diagram';
  diagram.setAttribute('role', 'figure');
  diagram.setAttribute('aria-label', 'LSM Tree Architecture Diagram showing data flow from Write-Ahead Log through Memtables to SSTables on disk');

  // Write path entry
  const writePath = createWritePath();

  // Memory section (WAL, Memtable, Immutable)
  const memorySection = createMemorySection();

  // Arrow from memory to disk
  const memToDiskArrow = createArrow('immutable', 'l0', 'Flush to Disk');
  memToDiskArrow.className += ' arch-arrow-flush-disk';

  // Disk section (SSTables)
  const diskSection = createDiskSection();

  diagram.appendChild(writePath);
  diagram.appendChild(memorySection);
  diagram.appendChild(memToDiskArrow);
  diagram.appendChild(diskSection);

  return diagram;
}

export function renderArchitecture(): HTMLElement {
  const section = document.createElement('section');
  section.id = 'architecture';
  section.className = 'architecture-section';
  section.setAttribute('aria-labelledby', 'architecture-heading');

  const container = document.createElement('div');
  container.className = 'container';

  const heading = document.createElement('h2');
  heading.id = 'architecture-heading';
  heading.textContent = 'Architecture';

  const description = document.createElement('p');
  description.className = 'architecture-description';
  description.textContent = 'MirDB uses a Log-Structured Merge-tree (LSM tree) architecture for efficient write operations and durable storage.';

  const diagram = createLSMDiagram();

  // Legend
  const legend = document.createElement('div');
  legend.className = 'arch-legend';
  legend.setAttribute('role', 'note');
  legend.setAttribute('aria-label', 'Diagram legend');

  const legendItems = [
    { color: 'memory', label: 'Memory Components' },
    { color: 'disk', label: 'Disk Components' },
    { color: 'arrow', label: 'Data Flow' },
  ];

  legendItems.forEach(item => {
    const legendItem = document.createElement('div');
    legendItem.className = 'arch-legend-item';

    const colorBox = document.createElement('span');
    colorBox.className = `arch-legend-color arch-legend-${item.color}`;

    const labelSpan = document.createElement('span');
    labelSpan.className = 'arch-legend-label';
    labelSpan.textContent = item.label;

    legendItem.appendChild(colorBox);
    legendItem.appendChild(labelSpan);
    legend.appendChild(legendItem);
  });

  container.appendChild(heading);
  container.appendChild(description);
  container.appendChild(diagram);
  container.appendChild(legend);
  section.appendChild(container);

  return section;
}
