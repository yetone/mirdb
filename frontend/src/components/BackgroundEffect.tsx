import { motion } from 'framer-motion';

interface BackgroundEffectProps {
  className?: string;
  'data-testid'?: string;
}

/**
 * BackgroundEffect component provides an animated background visual effect
 * for the hero section. It renders floating gradient blobs that animate
 * in the background behind the main content.
 */
export default function BackgroundEffect({
  className = '',
  'data-testid': testId = 'background-effect',
}: BackgroundEffectProps) {
  return (
    <div
      data-testid={testId}
      className={`absolute inset-0 overflow-hidden pointer-events-none ${className}`}
      style={{ zIndex: 0 }}
      aria-hidden="true"
    >
      {/* Primary floating blob */}
      <motion.div
        data-testid="background-effect-blob-1"
        className="absolute w-72 h-72 rounded-full bg-primary/30 blur-3xl"
        style={{ top: '10%', left: '20%' }}
        animate={{
          x: [0, 30, -20, 0],
          y: [0, -20, 30, 0],
          scale: [1, 1.1, 0.9, 1],
        }}
        transition={{
          duration: 8,
          repeat: Infinity,
          ease: 'easeInOut',
        }}
      />

      {/* Secondary floating blob */}
      <motion.div
        data-testid="background-effect-blob-2"
        className="absolute w-96 h-96 rounded-full bg-secondary/20 blur-3xl"
        style={{ top: '30%', right: '10%' }}
        animate={{
          x: [0, -40, 20, 0],
          y: [0, 30, -30, 0],
          scale: [1, 0.95, 1.05, 1],
        }}
        transition={{
          duration: 10,
          repeat: Infinity,
          ease: 'easeInOut',
        }}
      />

      {/* Tertiary floating blob */}
      <motion.div
        data-testid="background-effect-blob-3"
        className="absolute w-64 h-64 rounded-full bg-accent/20 blur-3xl"
        style={{ bottom: '20%', left: '40%' }}
        animate={{
          x: [0, 20, -30, 0],
          y: [0, -25, 15, 0],
          scale: [1, 1.05, 0.95, 1],
        }}
        transition={{
          duration: 12,
          repeat: Infinity,
          ease: 'easeInOut',
        }}
      />
    </div>
  );
}
