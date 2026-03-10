/**
 * Background Effect Component
 * Owner: Scenario 12 - Visual Effects and Animations
 *
 * An animated background component that displays floating particles
 * or a gradient effect. Respects the user's prefers-reduced-motion
 * accessibility setting.
 */

import { useMemo } from 'react';
import { motion, useReducedMotion as useFramerReducedMotion } from 'framer-motion';
import { useReducedMotion } from '../hooks/useReducedMotion';

interface Particle {
  id: number;
  x: number;
  y: number;
  size: number;
  duration: number;
  delay: number;
}

interface BackgroundEffectProps {
  particleCount?: number;
  className?: string;
}

/**
 * BackgroundEffect renders an animated particle background.
 * Falls back to a static gradient when reduced motion is preferred.
 */
export function BackgroundEffect({
  particleCount = 50,
  className = '',
}: BackgroundEffectProps) {
  const prefersReducedMotion = useReducedMotion();
  const framerReducedMotion = useFramerReducedMotion();
  const shouldReduceMotion = prefersReducedMotion || framerReducedMotion;

  // Generate particles with deterministic positions
  const particles = useMemo<Particle[]>(() => {
    return Array.from({ length: particleCount }, (_, i) => ({
      id: i,
      x: Math.random() * 100,
      y: Math.random() * 100,
      size: Math.random() * 4 + 2,
      duration: Math.random() * 20 + 10,
      delay: Math.random() * 5,
    }));
  }, [particleCount]);

  // Render static gradient for reduced motion
  if (shouldReduceMotion) {
    return (
      <div
        className={`fixed inset-0 -z-10 overflow-hidden ${className}`}
        data-testid="background-effect"
        data-reduced-motion="true"
        aria-hidden="true"
      >
        <div className="absolute inset-0 bg-gradient-to-br from-primary/5 via-transparent to-secondary/5" />
      </div>
    );
  }

  return (
    <div
      className={`fixed inset-0 -z-10 overflow-hidden ${className}`}
      data-testid="background-effect"
      data-reduced-motion="false"
      aria-hidden="true"
    >
      {/* Gradient background */}
      <motion.div
        className="absolute inset-0 bg-gradient-to-br from-primary/5 via-transparent to-secondary/5"
        animate={{
          opacity: [0.5, 0.8, 0.5],
        }}
        transition={{
          duration: 8,
          repeat: Infinity,
          ease: 'easeInOut',
        }}
      />

      {/* Animated particles */}
      {particles.map((particle) => (
        <motion.div
          key={particle.id}
          className="absolute rounded-full bg-primary/20"
          data-testid={`particle-${particle.id}`}
          style={{
            width: particle.size,
            height: particle.size,
            left: `${particle.x}%`,
            top: `${particle.y}%`,
          }}
          animate={{
            y: [0, -30, 0],
            x: [0, 10, 0],
            opacity: [0.2, 0.5, 0.2],
            scale: [1, 1.2, 1],
          }}
          transition={{
            duration: particle.duration,
            delay: particle.delay,
            repeat: Infinity,
            ease: 'easeInOut',
          }}
        />
      ))}
    </div>
  );
}

export default BackgroundEffect;
