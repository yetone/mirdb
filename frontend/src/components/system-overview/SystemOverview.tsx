/**
 * System Overview Panel component.
 * Owner: Scenario 4 - System Overview Panel
 *
 * Displays server configuration fetched from /api/config.
 * Covers REQ-4.
 */

import { useState, useCallback } from 'react';
import type { ServerConfig } from '../../types';

export interface SystemOverviewProps {
  config: ServerConfig;
}

interface ConfigItem {
  key: keyof ServerConfig;
  label: string;
  tooltip: string;
  format?: (value: string | number) => string;
}

const CONFIG_ITEMS: ConfigItem[] = [
  {
    key: 'listen_address',
    label: 'Listen Address',
    tooltip: 'Network address and port the MirDB server listens on for client connections',
  },
  {
    key: 'max_lsm_levels',
    label: 'Max LSM Levels',
    tooltip: 'Maximum number of LSM tree levels before forcing major compaction',
  },
  {
    key: 'work_directory',
    label: 'Work Directory',
    tooltip: 'Directory where MirDB stores SSTable files, write-ahead logs, and metadata',
  },
  {
    key: 'sstable_size_mb',
    label: 'SSTable Max Size',
    tooltip: 'Maximum size of individual SSTable files before triggering compaction',
    format: (value) => `${value} MB`,
  },
  {
    key: 'memtable_size_mb',
    label: 'Memtable Max Size',
    tooltip: 'Maximum size of the in-memory memtable before flushing to disk as an SSTable',
    format: (value) => `${value} MB`,
  },
];

function Tooltip({ text, visible }: { text: string; visible: boolean }) {
  if (!visible) return null;
  return (
    <div
      className="system-overview__tooltip"
      data-testid="system-overview-tooltip"
      role="tooltip"
    >
      {text}
    </div>
  );
}

function ConfigRow({ item, value }: { item: ConfigItem; value: string | number }) {
  const [tooltipVisible, setTooltipVisible] = useState(false);

  const handleMouseEnter = useCallback(() => {
    setTooltipVisible(true);
  }, []);

  const handleMouseLeave = useCallback(() => {
    setTooltipVisible(false);
  }, []);

  const displayValue = item.format ? item.format(value) : String(value);

  return (
    <div
      className="system-overview__row"
      data-testid={`system-overview-row-${item.key}`}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
    >
      <dt className="system-overview__label" data-testid={`system-overview-label-${item.key}`}>
        {item.label}
      </dt>
      <dd className="system-overview__value" data-testid={`system-overview-value-${item.key}`}>
        {displayValue}
        <Tooltip text={item.tooltip} visible={tooltipVisible} />
      </dd>
    </div>
  );
}

export default function SystemOverview({ config }: SystemOverviewProps) {
  return (
    <section
      className="system-overview"
      aria-label="System Overview"
      data-testid="system-overview-section"
    >
      <div className="system-overview__container">
        <h2 className="system-overview__heading" data-testid="system-overview-heading">
          System Overview
        </h2>
        <dl className="system-overview__list" data-testid="system-overview-list">
          {CONFIG_ITEMS.map((item) => (
            <ConfigRow
              key={item.key}
              item={item}
              value={config[item.key]}
            />
          ))}
        </dl>
      </div>
    </section>
  );
}

export { CONFIG_ITEMS };
