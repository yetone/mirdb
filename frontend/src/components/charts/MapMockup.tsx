/**
 * Map Mockup Component.
 * Owner: Scenario 6 - Features Section with Analytics Showcase
 *
 * Static SVG world map for "Geographic Insights" feature.
 * NOT a dynamic react-simple-maps component - static SVG for performance.
 *
 * Props: { className?: string, animate?: boolean }
 */
import { motion } from 'framer-motion';

interface MapMockupProps {
  className?: string;
  animate?: boolean;
}

// Simplified world map paths (continents outlines)
const mapPaths = {
  northAmerica:
    'M 40 40 Q 50 35 65 38 L 80 45 Q 85 55 75 70 L 55 80 Q 45 75 40 60 Z',
  southAmerica:
    'M 60 85 Q 70 82 75 90 L 70 115 Q 65 125 55 120 L 50 100 Q 52 88 60 85 Z',
  europe:
    'M 120 35 Q 130 32 145 38 L 150 50 Q 145 55 135 52 L 120 45 Q 118 38 120 35 Z',
  africa:
    'M 125 55 Q 135 52 145 58 L 150 85 Q 145 100 130 95 L 120 75 Q 118 60 125 55 Z',
  asia:
    'M 150 30 Q 180 25 210 35 L 220 60 Q 210 75 185 70 L 160 60 Q 148 45 150 30 Z',
  australia:
    'M 190 90 Q 210 85 220 95 L 215 110 Q 205 115 195 110 L 188 98 Q 185 92 190 90 Z',
};

// Hotspot markers (cities with click activity)
const hotspots = [
  { cx: 55, cy: 55, label: 'NYC', intensity: 1 },
  { cx: 135, cy: 42, label: 'London', intensity: 0.8 },
  { cx: 185, cy: 50, label: 'Tokyo', intensity: 0.9 },
  { cx: 130, cy: 70, label: 'Lagos', intensity: 0.5 },
  { cx: 200, cy: 100, label: 'Sydney', intensity: 0.6 },
];

export function MapMockup({ className = '', animate = true }: MapMockupProps) {
  const continentVariants = {
    hidden: { opacity: 0, scale: 0.95 },
    visible: {
      opacity: 1,
      scale: 1,
      transition: { duration: 0.5, ease: 'easeOut' },
    },
  };

  const pulseVariants = {
    hidden: { scale: 0, opacity: 0 },
    visible: (i: number) => ({
      scale: 1,
      opacity: 1,
      transition: { delay: 0.3 + i * 0.15, duration: 0.4 },
    }),
  };

  return (
    <svg
      viewBox="0 0 260 140"
      className={`w-full h-auto ${className}`}
      aria-label="World map showing geographic click distribution"
      role="img"
      data-testid="map-mockup"
    >
      {/* Background */}
      <rect width="260" height="140" fill="none" />

      {/* Continent shapes */}
      <motion.g
        fill="currentColor"
        fillOpacity="0.15"
        stroke="currentColor"
        strokeOpacity="0.3"
        strokeWidth="1"
        variants={animate ? continentVariants : undefined}
        initial={animate ? 'hidden' : 'visible'}
        animate="visible"
      >
        {Object.entries(mapPaths).map(([name, d]) => (
          <path key={name} d={d} />
        ))}
      </motion.g>

      {/* Hotspot markers with pulse animation */}
      {hotspots.map((spot, i) => (
        <motion.g
          key={i}
          custom={i}
          variants={animate ? pulseVariants : undefined}
          initial={animate ? 'hidden' : 'visible'}
          animate="visible"
        >
          {/* Pulse ring */}
          <motion.circle
            cx={spot.cx}
            cy={spot.cy}
            r={8 * spot.intensity}
            fill="currentColor"
            fillOpacity="0.2"
            animate={
              animate
                ? {
                    scale: [1, 1.5, 1],
                    opacity: [0.3, 0, 0.3],
                  }
                : undefined
            }
            transition={{
              duration: 2,
              repeat: Infinity,
              delay: i * 0.3,
            }}
          />
          {/* Center dot */}
          <circle
            cx={spot.cx}
            cy={spot.cy}
            r={4 * spot.intensity}
            fill="currentColor"
          />
        </motion.g>
      ))}

      {/* Activity indicator legend */}
      <motion.g
        initial={animate ? { opacity: 0 } : { opacity: 1 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 1, duration: 0.3 }}
      >
        <text
          x="20"
          y="130"
          fontSize="8"
          fill="currentColor"
          opacity="0.5"
        >
          Activity Level
        </text>
        <circle cx="75" cy="127" r="3" fill="currentColor" opacity="0.4" />
        <circle cx="90" cy="127" r="4" fill="currentColor" opacity="0.6" />
        <circle cx="108" cy="127" r="5" fill="currentColor" opacity="0.8" />
      </motion.g>
    </svg>
  );
}
