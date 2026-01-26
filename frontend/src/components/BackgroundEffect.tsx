import { useEffect, useRef } from 'react'

export default function BackgroundEffect() {
  const canvasRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    // Simple animated background effect
    // In production, this would use Three.js for 3D effects
    const container = canvasRef.current
    if (!container) return

    // Create floating particles
    const particles: HTMLDivElement[] = []
    for (let i = 0; i < 20; i++) {
      const particle = document.createElement('div')
      particle.className = 'absolute rounded-full bg-primary/20 animate-pulse'
      particle.style.width = `${Math.random() * 10 + 5}px`
      particle.style.height = particle.style.width
      particle.style.left = `${Math.random() * 100}%`
      particle.style.top = `${Math.random() * 100}%`
      particle.style.animationDelay = `${Math.random() * 2}s`
      container.appendChild(particle)
      particles.push(particle)
    }

    return () => {
      particles.forEach((p) => p.remove())
    }
  }, [])

  return (
    <div
      ref={canvasRef}
      className="fixed inset-0 -z-10 overflow-hidden pointer-events-none"
      aria-hidden="true"
    />
  )
}
