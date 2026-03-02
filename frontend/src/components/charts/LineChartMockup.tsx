/**
 * Line Chart Mockup Component.
 * Owner: Scenario 6 - Features Section with Analytics Showcase
 *
 * Static SVG line chart for "Real-time Click Tracking" feature.
 * NOT a dynamic Recharts component - static SVG for performance.
 *
 * Props: { className?: string, animate?: boolean }
 */
import { motion } from 'framer-motion';

interface LineChartMockupProps {
  className?: string;
  animate?: boolean;
}

export function LineChartMockup({ className = '', animate = true }: LineChartMockupProps) {
  const pathVariants = {
    hidden: { pathLength: 0, opacity: 0 },
    visible: {
      pathLength: 1,
      opacity: 1,
      transition: { duration: 1.5, ease: 'easeInOut' },
    },
  };

  const dotVariants = {
    hidden: { scale: 0, opacity: 0 },
    visible: (i: number) => ({
      scale: 1,
      opacity: 1,
      transition: { delay: 0.3 + i * 0.1, duration: 0.3 },
    }),
  };

  // Chart data points for the line
  const points = [
    { x: 20, y: 120 },
    { x: 60, y: 80 },
    { x: 100, y: 100 },
    { x: 140, y: 60 },
    { x: 180, y: 70 },
    { x: 220, y: 40 },
    { x: 260, y: 50 },
  ];

  const pathD = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');

  return (
    <svg
      viewBox="0 0 280 160"
      className={`w-full h-auto ${className}`}
      aria-label="Line chart showing click tracking trend"
      role="img"
      data-testid="line-chart-mockup"
    >
      {/* Grid lines */}
      <g stroke="currentColor" strokeOpacity="0.1" strokeWidth="1">
        <line x1="20" y1="40" x2="260" y2="40" />
        <line x1="20" y1="80" x2="260" y2="80" />
        <line x1="20" y1="120" x2="260" y2="120" />
      </g>

      {/* Y-axis labels */}
      <g fill="currentColor" fontSize="10" opacity="0.5">
        <text x="10" y="44" textAnchor="end">300</text>
        <text x="10" y="84" textAnchor="end">200</text>
        <text x="10" y="124" textAnchor="end">100</text>
      </g>

      {/* Area fill gradient */}
      <defs>
        <linearGradient id="lineChartGradient" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="currentColor" stopOpacity="0.3" />
          <stop offset="100%" stopColor="currentColor" stopOpacity="0" />
        </linearGradient>
      </defs>

      {/* Area fill */}
      <motion.path
        d={`${pathD} L 260 140 L 20 140 Z`}
        fill="url(#lineChartGradient)"
        initial={animate ? { opacity: 0 } : { opacity: 1 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.5 }}
      />

      {/* Main line */}
      <motion.path
        d={pathD}
        fill="none"
        stroke="currentColor"
        strokeWidth="3"
        strokeLinecap="round"
        strokeLinejoin="round"
        variants={animate ? pathVariants : undefined}
        initial={animate ? 'hidden' : 'visible'}
        animate="visible"
      />

      {/* Data points */}
      {points.map((point, i) => (
        <motion.circle
          key={i}
          cx={point.x}
          cy={point.y}
          r="5"
          fill="currentColor"
          custom={i}
          variants={animate ? dotVariants : undefined}
          initial={animate ? 'hidden' : 'visible'}
          animate="visible"
        />
      ))}
    </svg>
  );
}
