/**
 * Text content constants for the landing page.
 *
 * Centralized content management for headlines, descriptions,
 * and feature copy. Makes content updates easy and enables
 * future localization.
 */

export const HERO_CONTENT = {
  headline: "Shorten URLs, Track Performance",
  tagline: "Create memorable short URLs and track every click with detailed analytics.",
  inputPlaceholder: "Paste your long URL here...",
  submitButton: "Shorten URL",
};

export const FEATURES_CONTENT = {
  title: "Features",
  items: [
    {
      id: "shorten",
      title: "URL Shortening",
      description: "Transform any long URL into a concise, shareable link in seconds.",
    },
    {
      id: "analytics",
      title: "Detailed Analytics",
      description: "Track clicks, referrers, devices, locations, and more.",
    },
    {
      id: "share",
      title: "Shareable Stats",
      description: "Share your analytics with anyone using public share tokens.",
    },
    {
      id: "secure",
      title: "Secure Authentication",
      description: "Your data is protected with JWT-based secure authentication.",
    },
  ],
};

export const HOW_IT_WORKS_CONTENT = {
  title: "How It Works",
  steps: [
    {
      number: 1,
      title: "Paste Your URL",
      description: "Enter any long URL you want to shorten.",
    },
    {
      number: 2,
      title: "Get Short Link",
      description: "Instantly receive a short, memorable link.",
    },
    {
      number: 3,
      title: "Track and Share",
      description: "Monitor clicks and share your link anywhere.",
    },
  ],
};

export const CTA_CONTENT = {
  headline: "Unlock Detailed Analytics",
  description: "Sign up for free to access comprehensive click tracking and analytics.",
  buttonText: "Sign Up Free",
};

export const FOOTER_CONTENT = {
  copyright: "URL Shortener",
  links: [
    { label: "Privacy Policy", href: "/privacy" },
    { label: "Terms of Service", href: "/terms" },
    { label: "Contact", href: "/contact" },
  ],
};

export const REGISTRATION_PROMPT_CONTENT = {
  message: "Create an account to track clicks and view detailed analytics",
  buttonText: "Sign Up",
};

export const ERROR_MESSAGES = {
  emptyUrl: "Please enter a URL",
  invalidUrl: "Please enter a valid URL",
  invalidProtocol: "URL must start with http:// or https://",
  networkError: "Unable to connect. Please check your internet connection and try again.",
  serverError: "Something went wrong. Please try again later.",
  timeout: "Request timed out. Please try again.",
};
