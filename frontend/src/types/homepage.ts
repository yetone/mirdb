/**
 * Homepage-specific type definitions.
 *
 * This file is created by the first scenario builder and
 * should contain types used across homepage components.
 */

export interface Feature {
  icon: string;
  title: string;
  description: string;
}

export interface HowItWorksStep {
  step: number;
  title: string;
  description: string;
  icon: string;
}

export interface HeroProps {
  isAuthenticated: boolean;
  onGetStarted: () => void;
  onLogin: () => void;
}
