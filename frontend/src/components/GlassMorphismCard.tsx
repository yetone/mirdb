import type { ReactNode } from 'react';
import { motion, type HTMLMotionProps } from 'framer-motion';

interface GlassMorphismCardProps extends HTMLMotionProps<'div'> {
  children: ReactNode;
}

export function GlassMorphismCard({
  children,
  className = '',
  ...props
}: GlassMorphismCardProps) {
  return (
    <motion.div
      className={`backdrop-blur-md bg-base-100/70 rounded-xl border border-base-content/10 shadow-lg ${className}`}
      {...props}
    >
      {children}
    </motion.div>
  );
}
