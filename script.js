// Smooth scrolling and interactive functionality for MirDB homepage

/**
 * Scrolls to a specific section of the page smoothly
 * @param {string} sectionId - The ID of the section to scroll to
 */
function scrollToSection(sectionId) {
    const section = document.getElementById(sectionId);

    if (section) {
        section.scrollIntoView({
            behavior: 'smooth',
            block: 'start'
        });

        // Optional: Add visual feedback for click
        const button = document.getElementById('get-started-btn');
        if (button) {
            button.style.transform = 'scale(0.95)';
            setTimeout(() => {
                button.style.transform = 'scale(1)';
            }, 100);
        }
    } else {
        console.warn(`Section with id '${sectionId}' not found`);
    }
}

/**
 * Initialize hero section animations and interactions
 */
function initHeroSection() {
    // Add event listeners for buttons
    const getStartedBtn = document.getElementById('get-started-btn');
    const githubBtn = document.getElementById('github-btn');

    if (getStartedBtn) {
        getStartedBtn.addEventListener('click', (e) => {
            // Prevent default if we need to handle it differently
            console.log('Get Started button clicked');
        });
    }

    if (githubBtn) {
        githubBtn.addEventListener('click', (e) => {
            console.log('GitHub button clicked - opening in new tab');
        });
    }

    // Intersection Observer for animations
    if ('IntersectionObserver' in window) {
        const heroContent = document.querySelector('.hero-content');

        if (heroContent) {
            const observer = new IntersectionObserver(
                (entries) => {
                    entries.forEach(entry => {
                        if (entry.isIntersecting) {
                            entry.target.classList.add('animate-in');
                        }
                    });
                },
                { threshold: 0.1 }
            );

            observer.observe(heroContent);
        }
    }

    // Load performance tracking
    if (performance && performance.timing) {
        window.addEventListener('load', () => {
            const loadTime = performance.timing.loadEventEnd - performance.timing.navigationStart;
            console.log(`Hero section loaded in ${loadTime}ms`);

            if (loadTime > 2000) {
                console.warn('Page load time exceeds 2 seconds target');
            }
        });
    }
}

/**
 * Debounce utility for performance optimization
 * @param {Function} func - The function to debounce
 * @param {number} wait - The wait time in milliseconds
 */
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

/**
 * Handle mobile menu toggle if needed
 */
function handleMobileMenu() {
    const menuToggle = document.getElementById('mobile-menu-toggle');
    const navigation = document.getElementById('navigation');

    if (menuToggle && navigation) {
        menuToggle.addEventListener('click', () => {
            navigation.classList.toggle('open');
        });
    }
}

/**
 * Initialize header scroll effects
 */
function initScrollEffects() {
    let ticking = false;

    function updateScrollPosition() {
        const heroSection = document.querySelector('.hero-section');
        const scrollY = window.scrollY;

        if (heroSection) {
            // Add parallax effect to hero background
            heroSection.style.transform = `translateY(${scrollY * 0.5}px)`;
        }

        ticking = false;
    }

    function requestTick() {
        if (!ticking) {
            window.requestAnimationFrame(updateScrollPosition);
            ticking = true;
        }
    }

    // Throttle scroll events for performance
    window.addEventListener('scroll', debounce(requestTick, 16));
}

/**
 * Initialize keyboard navigation
 */
function initKeyboardNavigation() {
    document.addEventListener('keydown', (e) => {
        // Enter key activates focused button
        if (e.key === 'Enter' && document.activeElement.classList.contains('btn')) {
            document.activeElement.click();
        }

        // Tab key navigation - ensure focus is visible
        if (e.key === 'Tab') {
            document.body.classList.add('keyboard-nav');
        }
    });

    // Remove keyboard nav class on mouse interaction
    document.addEventListener('mousedown', () => {
        document.body.classList.remove('keyboard-nav');
    });
}

/**
 * Initialize hero section after DOM is loaded
 */
function initHeroSectionWhenReady() {
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initCompleteHeroSection);
    } else {
        initCompleteHeroSection();
    }
}

/**
 * Complete initialization of hero section and related features
 */
function initCompleteHeroSection() {
    try {
        initHeroSection();
        handleMobileMenu();
        initScrollEffects();
        initKeyboardNavigation();

        // Log initialization
        console.log('Hero section initialized successfully');
    } catch (error) {
        console.error('Error initializing hero section:', error);
    }
}

// Auto-initialize when script loads
initHeroSectionWhenReady();

// Export functions for testing
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        scrollToSection,
        initHeroSection,
        debounce
    };
}
