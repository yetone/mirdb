/**
 * Pie Chart Mockup Component.
 * Owner: Scenario 6 - Features Section with Analytics Showcase
 *
 * Static SVG pie chart for "Device & Browser Breakdown" feature.
 * NOT a dynamic Recharts component - static SVG for performance.
 *
 * Props: { className?: string, animate?: boolean }
 */
import { motion } from 'framer-motion';

interface PieChartMockupProps {
  className?: string;
  animate?: boolean;
}

// Pie chart segment data
const segments = [
  { percent: 45, color: 'var(--color-primary, #6366f1)', label: 'Desktop' },
  { percent: 35, color: 'var(--color-secondary, #a855f7)', label: 'Mobile' },
  { percent: 20, color: 'var(--color-accent, #ec4899)', label: 'Tablet' },
];

function polarToCartesian(cx: number, cy: number, r: number, angle: number) {
  const rad = ((angle - 90) * Math.PI) / 180;
  return {
    x: cx + r * Math.cos(rad),
    y: cy + r * Math.sin(rad),
  };
}

function describeArc(cx: number, cy: number, r: number, startAngle: number, endAngle: number) {
  const start = polarToCartesian(cx, cy, r, endAngle);
  const end = polarToCartesian(cx, cy, r, startAngle);
  const largeArcFlag = endAngle - startAngle <= 180 ? 0 : 1;
  return `M ${cx} ${cy} L ${start.x} ${start.y} A ${r} ${r} 0 ${largeArcFlag} 0 ${end.x} ${end.y} Z`;
}

export function PieChartMockup({ className = '', animate = true }: PieChartMockupProps) {
  const cx = 80;
  const cy = 80;
  const radius = 60;

  let currentAngle = 0;
  const paths = segments.map((segment) => {
    const startAngle = currentAngle;
    const endAngle = currentAngle + (segment.percent / 100) * 360;
    currentAngle = endAngle;
    return {
      d: describeArc(cx, cy, radius, startAngle, endAngle),
      color: segment.color,
      label: segment.label,
      percent: segment.percent,
    };
  });

  const segmentVariants = {
    hidden: { scale: 0, opacity: 0 },
    visible: (i: number) => ({
      scale: 1,
      opacity: 1,
      transition: { delay: i * 0.2, duration: 0.5, ease: 'easeOut' },
    }),
  };

  return (
    <svg
      viewBox="0 0 220 160"
      className={`w-full h-auto ${className}`}
      aria-label="Pie chart showing device breakdown"
      role="img"
      data-testid="pie-chart-mockup"
    >
      {/* Pie segments */}
      <g>
        {paths.map((path, i) => (
          <motion.path
            key={i}
            d={path.d}
            fill={path.color}
            stroke="white"
            strokeWidth="2"
            custom={i}
            variants={animate ? segmentVariants : undefined}
            initial={animate ? 'hidden' : 'visible'}
            animate="visible"
            style={{ transformOrigin: `${cx}px ${cy}px` }}
          />
        ))}
      </g>

      {/* Legend */}
      <g transform="translate(160, 40)">
        {segments.map((segment, i) => (
          <motion.g
            key={i}
            initial={animate ? { opacity: 0, x: -10 } : { opacity: 1, x: 0 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ delay: 0.5 + i * 0.1, duration: 0.3 }}
          >
            <rect
              x="0"
              y={i * 30}
              width="12"
              height="12"
              rx="2"
              fill={segment.color}
            />
            <text
              x="18"
              y={i * 30 + 10}
              fontSize="10"
              fill="currentColor"
              opacity="0.8"
            >
              {segment.label}
            </text>
            <text
              x="18"
              y={i * 30 + 22}
              fontSize="9"
              fill="currentColor"
              opacity="0.5"
            >
              {segment.percent}%
            </text>
          </motion.g>
        ))}
      </g>
    </svg>
  );
}
