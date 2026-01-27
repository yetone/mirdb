import { motion } from 'framer-motion';

interface BackgroundEffectProps {
  className?: string;
}

export function BackgroundEffect({ className = '' }: BackgroundEffectProps) {
  return (
    <div
      data-testid="background-effect"
      className={`fixed inset-0 -z-10 overflow-hidden ${className}`}
      aria-hidden="true"
    >
      <div className="absolute inset-0 bg-gradient-to-br from-primary/5 via-transparent to-secondary/5" />
      <motion.div
        className="absolute top-1/4 -left-20 h-96 w-96 rounded-full bg-primary/10 blur-3xl"
        animate={{
          x: [0, 100, 0],
          y: [0, 50, 0],
        }}
        transition={{
          duration: 20,
          repeat: Infinity,
          ease: 'linear',
        }}
      />
      <motion.div
        className="absolute bottom-1/4 -right-20 h-96 w-96 rounded-full bg-secondary/10 blur-3xl"
        animate={{
          x: [0, -100, 0],
          y: [0, -50, 0],
        }}
        transition={{
          duration: 25,
          repeat: Infinity,
          ease: 'linear',
        }}
      />
    </div>
  );
}
