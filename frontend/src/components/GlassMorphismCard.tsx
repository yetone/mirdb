import React from 'react';
import { motion } from 'framer-motion';

interface GlassMorphismCardProps {
  children: React.ReactNode;
  className?: string;
  hover?: boolean;
  'data-testid'?: string;
}

/**
 * GlassMorphismCard component.
 *
 * A card with glass-effect styling that can be used throughout the application.
 * Provides backdrop blur and subtle borders for a modern glass appearance.
 */
export const GlassMorphismCard: React.FC<GlassMorphismCardProps> = ({
  children,
  className = '',
  hover = true,
  'data-testid': testId,
}) => {
  return (
    <motion.div
      className={`
        backdrop-blur-md
        bg-base-100/30
        border border-base-content/10
        rounded-2xl
        shadow-xl
        ${className}
      `}
      whileHover={hover ? { scale: 1.02, y: -4 } : undefined}
      transition={{ duration: 0.2 }}
      data-testid={testId}
    >
      {children}
    </motion.div>
  );
};

export default GlassMorphismCard;
