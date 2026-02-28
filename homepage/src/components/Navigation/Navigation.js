/**
 * Navigation JavaScript
 * Owner: Scenario 3 - Mobile Navigation Responsive
 *
 * Functions:
 * - toggleMobileMenu(): Opens/closes mobile nav
 * - closeMobileMenu(): Closes menu
 * - handleOutsideClick(): Close on outside click
 * - handleEscapeKey(): Close on Escape key
 */

class MobileNavigation {
  constructor() {
    this.navToggle = document.getElementById('nav-toggle');
    this.navMenu = document.getElementById('nav-menu');
    this.overlay = null;
    this.isOpen = false;

    if (this.navToggle && this.navMenu) {
      this.init();
    }
  }

  init() {
    this.createOverlay();
    this.bindEvents();
  }

  createOverlay() {
    this.overlay = document.createElement('div');
    this.overlay.className = 'nav__overlay';
    this.overlay.setAttribute('aria-hidden', 'true');
    document.body.appendChild(this.overlay);
  }

  bindEvents() {
    this.navToggle.addEventListener('click', () => this.toggleMobileMenu());
    this.overlay.addEventListener('click', () => this.closeMobileMenu());
    document.addEventListener('keydown', (e) => this.handleEscapeKey(e));

    const navLinks = this.navMenu.querySelectorAll('.nav__link');
    navLinks.forEach(link => {
      link.addEventListener('click', () => this.closeMobileMenu());
    });

    window.addEventListener('resize', () => this.handleResize());
  }

  toggleMobileMenu() {
    this.isOpen ? this.closeMobileMenu() : this.openMobileMenu();
  }

  openMobileMenu() {
    this.isOpen = true;
    this.navMenu.classList.add('is-open');
    this.overlay.classList.add('is-visible');
    this.navToggle.setAttribute('aria-expanded', 'true');
    document.body.style.overflow = 'hidden';

    const firstLink = this.navMenu.querySelector('.nav__link');
    if (firstLink) {
      firstLink.focus();
    }
  }

  closeMobileMenu() {
    this.isOpen = false;
    this.navMenu.classList.remove('is-open');
    this.overlay.classList.remove('is-visible');
    this.navToggle.setAttribute('aria-expanded', 'false');
    document.body.style.overflow = '';

    this.navToggle.focus();
  }

  handleEscapeKey(event) {
    if (event.key === 'Escape' && this.isOpen) {
      this.closeMobileMenu();
    }
  }

  handleResize() {
    const isMobile = window.innerWidth <= 768;
    if (!isMobile && this.isOpen) {
      this.closeMobileMenu();
    }
  }
}

document.addEventListener('DOMContentLoaded', () => {
  new MobileNavigation();
});

export { MobileNavigation };
