import React from 'react';
import { motion } from 'framer-motion';

interface BackgroundEffectProps {
  variant?: 'default' | 'subtle' | 'intense';
  className?: string;
}

export const BackgroundEffect: React.FC<BackgroundEffectProps> = ({
  variant = 'default',
  className = ''
}) => {
  const opacity = variant === 'subtle' ? 0.3 : variant === 'intense' ? 0.8 : 0.5;

  return (
    <div className={`absolute inset-0 overflow-hidden pointer-events-none ${className}`}>
      <motion.div
        className="absolute -top-1/2 -left-1/2 w-full h-full bg-gradient-to-br from-primary/20 to-secondary/20 rounded-full blur-3xl"
        animate={{
          x: [0, 100, 0],
          y: [0, 50, 0],
        }}
        transition={{
          duration: 20,
          repeat: Infinity,
          ease: 'linear',
        }}
        style={{ opacity }}
      />
      <motion.div
        className="absolute -bottom-1/2 -right-1/2 w-full h-full bg-gradient-to-tl from-accent/20 to-primary/20 rounded-full blur-3xl"
        animate={{
          x: [0, -100, 0],
          y: [0, -50, 0],
        }}
        transition={{
          duration: 25,
          repeat: Infinity,
          ease: 'linear',
        }}
        style={{ opacity }}
      />
    </div>
  );
};

export default BackgroundEffect;
