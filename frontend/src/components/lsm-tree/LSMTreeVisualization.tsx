/**
 * LSM Tree Visualization component.
 * Owner: Scenario 6 - LSM Tree Visualization
 *
 * Renders an SVG visualization of the LSM tree structure showing
 * memtable, immutable memtable, and SSTable levels with file counts.
 * Includes hover tooltips and colorblind-safe patterns.
 */

import React, { useState, useCallback } from 'react';
import type { LSMState, MemtableState, SSTableLevel } from '../../types';

interface LSMTreeVisualizationProps {
  state: LSMState;
}

interface TooltipState {
  visible: boolean;
  x: number;
  y: number;
  title: string;
  lines: string[];
}

const SVG_WIDTH = 720;
const SVG_HEIGHT = 420;
const BLOCK_HEIGHT = 56;
const MARGIN_X = 32;
const MARGIN_Y = 24;
const GAP_Y = 16;
const GAP_X = 12;

/**
 * Colorblind-safe palette with high WCAG contrast.
 * Each level gets a distinct color + pattern combination.
 */
const LEVEL_COLORS: Record<string, { fill: string; stroke: string; text: string }> = {
  memtable: { fill: '#dbeafe', stroke: '#2563eb', text: '#1e3a5f' },
  immutable: { fill: '#fef3c7', stroke: '#d97706', text: '#78350f' },
  level0: { fill: '#dcfce7', stroke: '#16a34a', text: '#14532d' },
  level1: { fill: '#f3e8ff', stroke: '#9333ea', text: '#581c87' },
  level2: { fill: '#fce7f3', stroke: '#db2777', text: '#831843' },
  level3: { fill: '#ffedd5', stroke: '#ea580c', text: '#7c2d12' },
  level4: { fill: '#ccfbf1', stroke: '#0d9488', text: '#134e4a' },
  level5: { fill: '#e0e7ff', stroke: '#4f46e5', text: '#312e81' },
  level6: { fill: '#ecfccb', stroke: '#65a30d', text: '#3f6212' },
};

/**
 * Pattern definitions for colorblind accessibility.
 * Each level type uses a distinct SVG pattern.
 */
function PatternDefs() {
  return (
    <defs>
      {/* Memtable: diagonal stripes (top-left to bottom-right) */}
      <pattern id="pattern-memtable" patternUnits="userSpaceOnUse" width="8" height="8">
        <rect width="8" height="8" fill={LEVEL_COLORS.memtable.fill} />
        <path d="M0,8 L8,0" stroke={LEVEL_COLORS.memtable.stroke} strokeWidth="1" opacity="0.3" />
      </pattern>

      {/* Immutable: horizontal stripes */}
      <pattern id="pattern-immutable" patternUnits="userSpaceOnUse" width="8" height="8">
        <rect width="8" height="8" fill={LEVEL_COLORS.immutable.fill} />
        <line x1="0" y1="4" x2="8" y2="4" stroke={LEVEL_COLORS.immutable.stroke} strokeWidth="1" opacity="0.3" />
      </pattern>

      {/* Level 0: dots */}
      <pattern id="pattern-level0" patternUnits="userSpaceOnUse" width="8" height="8">
        <rect width="8" height="8" fill={LEVEL_COLORS.level0.fill} />
        <circle cx="4" cy="4" r="1.5" fill={LEVEL_COLORS.level0.stroke} opacity="0.4" />
      </pattern>

      {/* Level 1: vertical stripes */}
      <pattern id="pattern-level1" patternUnits="userSpaceOnUse" width="8" height="8">
        <rect width="8" height="8" fill={LEVEL_COLORS.level1.fill} />
        <line x1="4" y1="0" x2="4" y2="8" stroke={LEVEL_COLORS.level1.stroke} strokeWidth="1" opacity="0.3" />
      </pattern>

      {/* Level 2: crosshatch */}
      <pattern id="pattern-level2" patternUnits="userSpaceOnUse" width="8" height="8">
        <rect width="8" height="8" fill={LEVEL_COLORS.level2.fill} />
        <path d="M0,0 L8,8 M8,0 L0,8" stroke={LEVEL_COLORS.level2.stroke} strokeWidth="0.8" opacity="0.25" />
      </pattern>

      {/* Level 3: diagonal cross (other direction) */}
      <pattern id="pattern-level3" patternUnits="userSpaceOnUse" width="8" height="8">
        <rect width="8" height="8" fill={LEVEL_COLORS.level3.fill} />
        <path d="M0,0 L8,8 M0,8 L8,0" stroke={LEVEL_COLORS.level3.stroke} strokeWidth="0.8" opacity="0.25" />
      </pattern>

      {/* Level 4: checkerboard */}
      <pattern id="pattern-level4" patternUnits="userSpaceOnUse" width="8" height="8">
        <rect width="8" height="8" fill={LEVEL_COLORS.level4.fill} />
        <rect x="0" y="0" width="4" height="4" fill={LEVEL_COLORS.level4.stroke} opacity="0.15" />
        <rect x="4" y="4" width="4" height="4" fill={LEVEL_COLORS.level4.stroke} opacity="0.15" />
      </pattern>

      {/* Level 5: wavy lines */}
      <pattern id="pattern-level5" patternUnits="userSpaceOnUse" width="12" height="8">
        <rect width="12" height="8" fill={LEVEL_COLORS.level5.fill} />
        <path d="M0,4 Q3,0 6,4 Q9,8 12,4" stroke={LEVEL_COLORS.level5.stroke} strokeWidth="0.8" fill="none" opacity="0.3" />
      </pattern>

      {/* Level 6: grid */}
      <pattern id="pattern-level6" patternUnits="userSpaceOnUse" width="8" height="8">
        <rect width="8" height="8" fill={LEVEL_COLORS.level6.fill} />
        <rect x="0" y="0" width="8" height="8" stroke={LEVEL_COLORS.level6.stroke} strokeWidth="0.5" opacity="0.2" fill="none" />
      </pattern>
    </defs>
  );
}

function MemtableBlock(props: {
  label: string;
  memtable: MemtableState;
  x: number;
  y: number;
  width: number;
  patternId: string;
  colorKey: string;
  onHover: React.Dispatch<React.SetStateAction<TooltipState | null>>;
}) {
  const { label, memtable, x, y, width, patternId, colorKey, onHover } = props;
  const colors = LEVEL_COLORS[colorKey];

  const handleMouseEnter = useCallback(
    (e: React.MouseEvent) => {
      onHover({
        visible: true,
        x: e.clientX,
        y: e.clientY,
        title: label,
        lines: [
          `Keys: ${memtable.key_count}`,
          `Size: ${formatBytes(memtable.size_bytes)}`,
          `Location: RAM (in-memory skip-list)`,
        ],
      });
    },
    [label, memtable, onHover]
  );

  const handleMouseMove = useCallback(
    (e: React.MouseEvent) => {
      onHover(prev =>
        prev
          ? { ...prev, x: e.clientX, y: e.clientY }
          : null
      );
    },
    [onHover]
  );

  const handleMouseLeave = useCallback(() => {
    onHover(null);
  }, [onHover]);

  return (
    <g
      data-testid="memtable-block"
      onMouseEnter={handleMouseEnter}
      onMouseMove={handleMouseMove}
      onMouseLeave={handleMouseLeave}
    >
      <rect
        x={x}
        y={y}
        width={width}
        height={BLOCK_HEIGHT}
        rx={6}
        fill={`url(#${patternId})`}
        stroke={colors.stroke}
        strokeWidth={2}
      />
      <text
        x={x + width / 2}
        y={y + 20}
        textAnchor="middle"
        fill={colors.text}
        fontSize="13"
        fontWeight="600"
      >
        {label}
      </text>
      <text
        x={x + width / 2}
        y={y + 40}
        textAnchor="middle"
        fill={colors.text}
        fontSize="12"
      >
        {`${memtable.key_count} key${memtable.key_count === 1 ? '' : 's'}`}
      </text>
    </g>
  );
}

function SSTableLevelBlock(props: {
  level: SSTableLevel;
  x: number;
  y: number;
  width: number;
  onHover: React.Dispatch<React.SetStateAction<TooltipState | null>>;
}) {
  const { level, x, y, width, onHover } = props;
  const colorKey = `level${Math.min(level.level, 6)}`;
  const colors = LEVEL_COLORS[colorKey] || LEVEL_COLORS.level0;
  const patternId = `pattern-${colorKey}`;

  const handleMouseEnter = useCallback(
    (e: React.MouseEvent) => {
      onHover({
        visible: true,
        x: e.clientX,
        y: e.clientY,
        title: `Level ${level.level}`,
        lines: [
          `Files: ${level.file_count}`,
          `Total size: ${formatBytes(level.total_size_bytes)}`,
          `Compaction: ${level.file_count > 4 ? 'needed' : 'ok'}`,
        ],
      });
    },
    [level, onHover]
  );

  const handleMouseMove = useCallback(
    (e: React.MouseEvent) => {
      onHover(prev => (prev ? { ...prev, x: e.clientX, y: e.clientY } : null));
    },
    [onHover]
  );

  const handleMouseLeave = useCallback(() => {
    onHover(null);
  }, [onHover]);

  return (
    <g
      data-testid={`sstable-level-${level.level}`}
      onMouseEnter={handleMouseEnter}
      onMouseMove={handleMouseMove}
      onMouseLeave={handleMouseLeave}
    >
      <rect
        x={x}
        y={y}
        width={width}
        height={BLOCK_HEIGHT}
        rx={6}
        fill={`url(#${patternId})`}
        stroke={colors.stroke}
        strokeWidth={2}
      />
      <text
        x={x + width / 2}
        y={y + 20}
        textAnchor="middle"
        fill={colors.text}
        fontSize="13"
        fontWeight="600"
      >
        {`Level ${level.level}`}
      </text>
      <text
        x={x + width / 2}
        y={y + 40}
        textAnchor="middle"
        fill={colors.text}
        fontSize="12"
      >
        {`${level.file_count} file${level.file_count === 1 ? '' : 's'}`}
      </text>
    </g>
  );
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(1))} ${sizes[i]}`;
}

function Tooltip(props: { tooltip: TooltipState | null }) {
  const { tooltip } = props;
  if (!tooltip || !tooltip.visible) return null;

  return (
    <div
      data-testid="lsm-tooltip"
      style={{
        position: 'fixed',
        left: tooltip.x + 12,
        top: tooltip.y - 12,
        backgroundColor: 'rgba(17, 24, 39, 0.95)',
        color: '#f9fafb',
        padding: '8px 12px',
        borderRadius: '6px',
        fontSize: '13px',
        lineHeight: '1.5',
        pointerEvents: 'none',
        zIndex: 1000,
        boxShadow: '0 4px 6px -1px rgba(0,0,0,0.3)',
        maxWidth: '280px',
      }}
      role="tooltip"
    >
      <div style={{ fontWeight: 600, marginBottom: '4px' }}>{tooltip.title}</div>
      {tooltip.lines.map((line, i) => (
        <div key={i}>{line}</div>
      ))}
    </div>
  );
}

export default function LSMTreeVisualization(props: LSMTreeVisualizationProps) {
  const { state } = props;
  const [tooltip, setTooltip] = useState<TooltipState | null>(null);

  const contentWidth = SVG_WIDTH - MARGIN_X * 2;
  const halfWidth = (contentWidth - GAP_X) / 2;

  // Layout rows
  const row1Y = MARGIN_Y + 24;
  const row2Y = row1Y + BLOCK_HEIGHT + GAP_Y + 8;

  // Level blocks are arranged in a grid (max 4 per row)
  const levelsPerRow = 4;
  const levelBlockWidth = (contentWidth - GAP_X * (levelsPerRow - 1)) / levelsPerRow;

  return (
    <section
      data-testid="lsm-tree-section"
      aria-label="LSM Tree Visualization"
      style={{ padding: '24px 0' }}
    >
      <h2
        data-testid="lsm-tree-title"
        style={{
          fontSize: '20px',
          fontWeight: 700,
          marginBottom: '16px',
          paddingLeft: MARGIN_X,
        }}
      >
        LSM Tree Visualization
      </h2>

      <svg
        data-testid="lsm-tree-svg"
        width="100%"
        viewBox={`0 0 ${SVG_WIDTH} ${SVG_HEIGHT}`}
        role="img"
        aria-label="Diagram showing LSM tree structure with memtable and SSTable levels"
      >
        <PatternDefs />

        {/* Memtable */}
        <MemtableBlock
          label="Memtable"
          memtable={state.memtable}
          x={MARGIN_X}
          y={row1Y}
          width={state.immutable_memtable ? halfWidth : contentWidth}
          patternId="pattern-memtable"
          colorKey="memtable"
          onHover={setTooltip}
        />

        {/* Immutable Memtable */}
        {state.immutable_memtable && (
          <MemtableBlock
            label="Immutable Memtable"
            memtable={state.immutable_memtable}
            x={MARGIN_X + halfWidth + GAP_X}
            y={row1Y}
            width={halfWidth}
            patternId="pattern-immutable"
            colorKey="immutable"
            onHover={setTooltip}
          />
        )}

        {/* Divider label */}
        <text
          x={MARGIN_X}
          y={row1Y + BLOCK_HEIGHT + GAP_Y}
          fill="#6b7280"
          fontSize="11"
          fontWeight="500"
        >
          SSTable Levels
        </text>

        {/* SSTable Levels */}
        {state.levels.map((level, index) => {
          const col = index % levelsPerRow;
          const row = Math.floor(index / levelsPerRow);
          const lx = MARGIN_X + col * (levelBlockWidth + GAP_X);
          const ly = row2Y + row * (BLOCK_HEIGHT + GAP_Y);
          return (
            <SSTableLevelBlock
              key={level.level}
              level={level}
              x={lx}
              y={ly}
              width={levelBlockWidth}
              onHover={setTooltip}
            />
          );
        })}
      </svg>

      <Tooltip tooltip={tooltip} />
    </section>
  );
}

export { formatBytes };
