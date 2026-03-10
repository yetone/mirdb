/**
 * Registration Prompt Component
 * Owner: Scenario 11 - Post-Shortening Registration Prompt
 *
 * Displayed after anonymous user successfully shortens a URL.
 * Encourages registration for analytics access.
 *
 * Features:
 * - Message about analytics benefits
 * - Sign Up button linking to /register
 * - Non-blocking (doesn't hide the short URL result)
 */

import { motion } from 'framer-motion';
import { Link } from 'react-router-dom';
import { REGISTRATION_PROMPT_CONTENT } from '../../constants/landingContent';

// Animation variants for entrance animation
const promptVariants = {
  hidden: { opacity: 0, y: 10 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.4,
      ease: 'easeOut',
      delay: 0.3, // Slight delay to appear after the short URL result
    },
  },
} as const;

// Button hover animation
const buttonVariants = {
  initial: { scale: 1 },
  hover: {
    scale: 1.05,
    transition: {
      type: 'spring',
      stiffness: 400,
      damping: 15,
    },
  },
  tap: { scale: 0.98 },
} as const;

/**
 * RegistrationPrompt displays a call-to-action encouraging user registration
 * after successfully shortening a URL. This component is non-blocking and
 * appears alongside the short URL result.
 */
export function RegistrationPrompt() {
  return (
    <motion.div
      className="w-full p-4 mt-4 rounded-lg bg-base-200/50 border border-base-300/50"
      role="complementary"
      aria-label="Registration prompt"
      data-testid="registration-prompt"
      variants={promptVariants}
      initial="hidden"
      animate="visible"
    >
      <div className="flex flex-col sm:flex-row items-center justify-between gap-4">
        {/* Message about analytics benefits */}
        <p
          className="text-base-content/80 text-sm sm:text-base text-center sm:text-left"
          data-testid="registration-prompt-message"
        >
          {REGISTRATION_PROMPT_CONTENT.message}
        </p>

        {/* Sign Up Button */}
        <motion.div
          variants={buttonVariants}
          initial="initial"
          whileHover="hover"
          whileTap="tap"
          className="shrink-0"
        >
          <Link
            to="/register"
            className="btn btn-primary btn-sm sm:btn-md"
            data-testid="registration-prompt-signup-button"
          >
            {REGISTRATION_PROMPT_CONTENT.buttonText}
          </Link>
        </motion.div>
      </div>
    </motion.div>
  );
}
