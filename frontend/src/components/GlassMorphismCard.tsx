/**
 * Glassmorphism Card Component
 * Owner: Scenario 12 - Visual Effects and Animations
 *
 * A card component with glassmorphism styling (frosted glass effect)
 * and subtle hover animations. Respects reduced motion preferences.
 */

import { type ReactNode, type MouseEventHandler } from 'react';
import { motion } from 'framer-motion';
import { useReducedMotion } from '../hooks/useReducedMotion';

interface GlassMorphismCardProps {
  children: ReactNode;
  className?: string;
  hoverEffect?: boolean;
  glowColor?: 'primary' | 'secondary' | 'accent' | 'none';
  padding?: 'none' | 'sm' | 'md' | 'lg';
  onClick?: MouseEventHandler<HTMLDivElement>;
}

const paddingClasses: Record<string, string> = {
  none: '',
  sm: 'p-3',
  md: 'p-5',
  lg: 'p-8',
};

const glowColorClasses: Record<string, string> = {
  primary: 'hover:shadow-primary/20',
  secondary: 'hover:shadow-secondary/20',
  accent: 'hover:shadow-accent/20',
  none: '',
};

/**
 * GlassMorphismCard renders a frosted-glass style card with
 * optional hover animations and glow effects.
 */
export function GlassMorphismCard({
  children,
  className = '',
  hoverEffect = true,
  glowColor = 'primary',
  padding = 'md',
  onClick,
}: GlassMorphismCardProps) {
  const prefersReducedMotion = useReducedMotion();

  const baseClasses = `
    relative overflow-hidden
    rounded-xl
    backdrop-blur-md
    bg-base-200/50
    border border-base-content/10
    shadow-lg
    transition-shadow duration-300
  `;

  // Animation variants for hover effects
  const cardVariants = {
    initial: {
      scale: 1,
      boxShadow: '0 10px 40px rgba(0, 0, 0, 0.1)',
    },
    hover:
      hoverEffect && !prefersReducedMotion
        ? {
            scale: 1.02,
            boxShadow: '0 20px 50px rgba(0, 0, 0, 0.15)',
          }
        : {},
  };

  // If reduced motion, use simpler component
  if (prefersReducedMotion) {
    return (
      <div
        className={`
          ${baseClasses}
          ${paddingClasses[padding]}
          ${hoverEffect ? glowColorClasses[glowColor] : ''}
          ${className}
        `}
        data-testid="glass-card"
        data-reduced-motion="true"
        data-hover-effect={hoverEffect ? 'true' : 'false'}
        onClick={onClick}
      >
        {/* Highlight overlay (CSS-only hover effect) */}
        {hoverEffect && (
          <div
            className="absolute inset-0 bg-primary/5 opacity-0 hover:opacity-100 transition-opacity duration-300 pointer-events-none"
            aria-hidden="true"
          />
        )}
        <div className="relative z-10">{children}</div>
      </div>
    );
  }

  return (
    <motion.div
      className={`
        ${baseClasses}
        ${paddingClasses[padding]}
        ${hoverEffect ? glowColorClasses[glowColor] : ''}
        ${className}
      `}
      data-testid="glass-card"
      data-reduced-motion="false"
      data-hover-effect={hoverEffect ? 'true' : 'false'}
      variants={cardVariants}
      initial="initial"
      whileHover="hover"
      transition={{
        type: 'spring',
        stiffness: 300,
        damping: 20,
      }}
      onClick={onClick}
    >
      {/* Animated highlight overlay */}
      {hoverEffect && (
        <motion.div
          className="absolute inset-0 bg-primary/5 pointer-events-none"
          initial={{ opacity: 0 }}
          whileHover={{ opacity: 1 }}
          transition={{ duration: 0.3 }}
          aria-hidden="true"
        />
      )}

      {/* Shine effect on hover */}
      {hoverEffect && (
        <motion.div
          className="absolute inset-0 bg-gradient-to-r from-transparent via-white/5 to-transparent pointer-events-none"
          initial={{ x: '-100%', opacity: 0 }}
          whileHover={{
            x: '100%',
            opacity: 1,
            transition: {
              duration: 0.6,
              ease: 'easeInOut',
            },
          }}
          aria-hidden="true"
        />
      )}

      <div className="relative z-10">{children}</div>
    </motion.div>
  );
}

export default GlassMorphismCard;
