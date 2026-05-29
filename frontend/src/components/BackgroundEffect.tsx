/**
 * BackgroundEffect Component
 * Visual ambient background with animated gradient orbs.
 * Respects prefers-reduced-motion for accessibility.
 */

import { useEffect, useRef, useState } from 'react';

function useReducedMotion(): boolean {
  const [reduced, setReduced] = useState(false);

  useEffect(() => {
    const mql = window.matchMedia('(prefers-reduced-motion: reduce)');
    setReduced(mql.matches);

    const handler = (e: MediaQueryListEvent) => setReduced(e.matches);
    mql.addEventListener('change', handler);
    return () => mql.removeEventListener('change', handler);
  }, []);

  return reduced;
}

export default function BackgroundEffect() {
  const reducedMotion = useReducedMotion();
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const animRef = useRef<number>(0);

  useEffect(() => {
    if (reducedMotion) return;

    const canvas = canvasRef.current;
    if (!canvas) return;

    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    let width = window.innerWidth;
    let height = window.innerHeight;

    const resize = () => {
      width = window.innerWidth;
      height = window.innerHeight;
      canvas.width = width;
      canvas.height = height;
    };
    resize();
    window.addEventListener('resize', resize);

    const orbs = [
      { x: width * 0.2, y: height * 0.3, r: 180, dx: 0.3, dy: 0.2, color: 'rgba(59, 130, 246, 0.08)' },
      { x: width * 0.7, y: height * 0.5, r: 220, dx: -0.2, dy: 0.3, color: 'rgba(139, 92, 246, 0.08)' },
      { x: width * 0.5, y: height * 0.8, r: 160, dx: 0.15, dy: -0.25, color: 'rgba(236, 72, 153, 0.06)' },
    ];

    const animate = () => {
      ctx.clearRect(0, 0, width, height);

      for (const orb of orbs) {
        orb.x += orb.dx;
        orb.y += orb.dy;

        if (orb.x < -orb.r) orb.x = width + orb.r;
        if (orb.x > width + orb.r) orb.x = -orb.r;
        if (orb.y < -orb.r) orb.y = height + orb.r;
        if (orb.y > height + orb.r) orb.y = -orb.r;

        const gradient = ctx.createRadialGradient(orb.x, orb.y, 0, orb.x, orb.y, orb.r);
        gradient.addColorStop(0, orb.color);
        gradient.addColorStop(1, 'rgba(0,0,0,0)');
        ctx.fillStyle = gradient;
        ctx.fillRect(0, 0, width, height);
      }

      animRef.current = requestAnimationFrame(animate);
    };

    animRef.current = requestAnimationFrame(animate);

    return () => {
      cancelAnimationFrame(animRef.current);
      window.removeEventListener('resize', resize);
    };
  }, [reducedMotion]);

  if (reducedMotion) {
    return (
      <div
        data-testid="background-effect"
        data-reduced-motion="true"
        aria-hidden="true"
        className="background-effect fixed inset-0 -z-10 pointer-events-none"
        style={{
          background: 'linear-gradient(135deg, rgba(59,130,246,0.03) 0%, rgba(139,92,246,0.03) 50%, rgba(236,72,153,0.02) 100%)',
        }}
      />
    );
  }

  return (
    <canvas
      ref={canvasRef}
      data-testid="background-effect"
      data-reduced-motion="false"
      aria-hidden="true"
      className="background-effect fixed inset-0 -z-10 pointer-events-none"
    />
  );
}

export { useReducedMotion };
