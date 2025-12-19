import './Hero.css'

export interface CTAButton {
  label: string
  href: string
  primary: boolean
}

export interface HeroProps {
  productName: string
  tagline: string
  description: string
  ctaButtons: CTAButton[]
}

export function Hero({ productName, tagline, description, ctaButtons }: HeroProps) {
  return (
    <section className="hero">
      <div className="hero-content">
        <h1 className="hero-title">{productName}</h1>
        <p className="hero-tagline">{tagline}</p>
        <p className="hero-description">{description}</p>
        <div className="hero-cta-container">
          {ctaButtons.map((button, index) => (
            <a
              key={index}
              href={button.href}
              className={button.primary ? 'cta-primary' : 'cta-secondary'}
            >
              {button.label}
            </a>
          ))}
        </div>
      </div>
    </section>
  )
}
