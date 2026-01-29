/**
 * Hero Logo Component (React Island).
 * Owner: Scenario 1 - Hero Section Implementation
 *
 * Expected features:
 * - Display MirDB logo or animated ASCII art
 * - Terminal-style animation effect
 * - Respect prefers-reduced-motion for animations
 */

import { useState, useEffect } from 'react';

interface HeroLogoProps {
  className?: string;
}

const ASCII_ART = `
███╗   ███╗██╗██████╗ ██████╗ ██████╗
████╗ ████║██║██╔══██╗██╔══██╗██╔══██╗
██╔████╔██║██║██████╔╝██║  ██║██████╔╝
██║╚██╔╝██║██║██╔══██╗██║  ██║██╔══██╗
██║ ╚═╝ ██║██║██║  ██║██████╔╝██████╔╝
╚═╝     ╚═╝╚═╝╚═╝  ╚═╝╚═════╝ ╚═════╝
`.trim();

export function HeroLogo({ className = '' }: HeroLogoProps) {
  const [displayedText, setDisplayedText] = useState('');
  const [isAnimationComplete, setIsAnimationComplete] = useState(false);
  const [prefersReducedMotion, setPrefersReducedMotion] = useState(false);

  useEffect(() => {
    // Check for reduced motion preference
    const mediaQuery = window.matchMedia('(prefers-reduced-motion: reduce)');
    setPrefersReducedMotion(mediaQuery.matches);

    const handleChange = (e: MediaQueryListEvent) => {
      setPrefersReducedMotion(e.matches);
    };

    mediaQuery.addEventListener('change', handleChange);
    return () => mediaQuery.removeEventListener('change', handleChange);
  }, []);

  useEffect(() => {
    // If reduced motion is preferred, show full text immediately
    if (prefersReducedMotion) {
      setDisplayedText(ASCII_ART);
      setIsAnimationComplete(true);
      return;
    }

    // Animate the ASCII art typing effect
    let currentIndex = 0;
    const typingSpeed = 5; // ms per character

    const animationInterval = setInterval(() => {
      if (currentIndex <= ASCII_ART.length) {
        setDisplayedText(ASCII_ART.slice(0, currentIndex));
        currentIndex++;
      } else {
        setIsAnimationComplete(true);
        clearInterval(animationInterval);
      }
    }, typingSpeed);

    return () => clearInterval(animationInterval);
  }, [prefersReducedMotion]);

  return (
    <div
      className={`font-mono text-accent text-center ${className}`}
      aria-label="MirDB logo"
      role="img"
    >
      <pre
        className="text-xs sm:text-sm md:text-base lg:text-lg leading-tight inline-block text-left"
        aria-hidden="true"
      >
        {displayedText}
        {!isAnimationComplete && (
          <span className="inline-block w-2 h-4 bg-accent animate-pulse ml-0.5" />
        )}
      </pre>
    </div>
  );
}

export default HeroLogo;
