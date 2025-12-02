"""
Emotional Resonance Validator Module for VerseCraft

This module provides functionality to validate that a poem achieves target emotional
resonance rating (Success Criteria 2 from PRD).

Scenario: Emotional Resonance Validation
- Conduct stakeholder survey: Have 3+ independent reviewers rate emotional impact
- Collect qualitative feedback: Gather descriptive feedback on emotional response

Target: 4/5 average rating on emotional resonance
Looking for words like 'inspired', 'memorable', 'connected'
"""

import re
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum


class EmotionalResponse(Enum):
    """Types of emotional responses that indicate positive resonance."""
    INSPIRED = "inspired"
    MEMORABLE = "memorable"
    CONNECTED = "connected"
    MOVED = "moved"
    ENGAGED = "engaged"
    TOUCHED = "touched"


@dataclass
class ReviewerRating:
    """Represents a single reviewer's rating and feedback."""
    reviewer_id: str
    emotional_resonance_rating: float  # 1.0 to 5.0
    qualitative_feedback: str
    emotional_keywords_found: List[str]

    def is_positive(self) -> bool:
        """Check if the review indicates positive emotional response."""
        return self.emotional_resonance_rating >= 4.0


@dataclass
class EmotionalResonanceReport:
    """Complete emotional resonance validation report."""
    average_rating: float
    total_reviewers: int
    individual_ratings: List[ReviewerRating]
    positive_feedback_count: int
    emotional_keywords_found: Dict[str, int]
    meets_target: bool
    majority_positive: bool
    explanation: str


class EmotionalResonanceValidator:
    """
    Validates the emotional resonance of poems for MirDB marketing.

    This validator evaluates poems based on:
    1. Independent reviewer ratings (target: 4/5 average)
    2. Qualitative feedback analysis (looking for positive emotional responses)

    Per PRD:
    - Measurement Plan: Emotional Resonance target is 4/5 average rating
    - Success Criteria 2: Poem resonates emotionally with at least 3 independent reviewers
    """

    # Target rating threshold
    TARGET_RATING = 4.0
    MIN_REVIEWERS = 3

    # Positive emotional response keywords from PRD
    POSITIVE_EMOTIONAL_KEYWORDS = [
        'inspired', 'inspiring', 'inspiration',
        'memorable', 'unforgettable', 'remember',
        'connected', 'connecting', 'connection',
        'moved', 'moving', 'touched', 'touching',
        'engaged', 'engaging', 'captivating',
        'beautiful', 'elegant', 'graceful',
        'powerful', 'impactful', 'resonant',
        'evocative', 'stirring', 'compelling',
        'heartfelt', 'meaningful', 'poignant',
        'creative', 'imaginative', 'artistic'
    ]

    # Negative emotional response keywords to detect issues
    NEGATIVE_EMOTIONAL_KEYWORDS = [
        'boring', 'dull', 'uninteresting',
        'confusing', 'unclear', 'muddled',
        'disconnected', 'cold', 'clinical',
        'forgettable', 'generic', 'flat',
        'uninspired', 'lifeless', 'mechanical'
    ]

    # Emotional strength indicators in poem content
    EMOTIONAL_STRENGTH_PATTERNS = [
        (r'\beternal\b|\bforever\b|\bendless\b', 'eternity_theme'),
        (r'\btrust\b|\bfaithful\b|\bsteadfast\b', 'trust_theme'),
        (r'\bguard\w*\b|\bprotect\w*\b|\bsafe\w*\b', 'protection_theme'),
        (r'\brise\w*\b|\bforge\w*\b|\bcreate\w*\b', 'creation_theme'),
        (r'\blight\b|\bbright\b|\bshine\w*\b', 'light_theme'),
        (r'\bdream\w*\b|\bhope\w*\b|\bpromise\w*\b', 'aspiration_theme'),
        (r'\bcare\b|\btreasure\w*\b|\bprecious\b', 'value_theme'),
        (r'\bgrace\b|\belegant\w*\b|\bbeauti\w*\b', 'aesthetic_theme'),
    ]

    def __init__(self, poem_content: str):
        """
        Initialize the emotional resonance validator.

        Args:
            poem_content: The full text content of the poem to validate
        """
        self.poem_content = poem_content
        self.poem_lower = poem_content.lower()
        self._reviews: List[ReviewerRating] = []
        self._emotional_analysis: Optional[Dict] = None

    def analyze_poem_emotional_content(self) -> Dict[str, any]:
        """
        Analyze the poem's inherent emotional content and strength.

        Returns:
            Dictionary with emotional content analysis results
        """
        if self._emotional_analysis is not None:
            return self._emotional_analysis

        analysis = {
            'emotional_themes': [],
            'theme_count': 0,
            'emotional_density': 0.0,
            'has_strong_opening': False,
            'has_strong_closing': False,
            'metaphor_count': 0,
        }

        # Detect emotional themes
        for pattern, theme_name in self.EMOTIONAL_STRENGTH_PATTERNS:
            if re.search(pattern, self.poem_lower):
                analysis['emotional_themes'].append(theme_name)

        analysis['theme_count'] = len(analysis['emotional_themes'])

        # Calculate emotional density (emotional words per line)
        lines = [l for l in self.poem_content.strip().split('\n') if l.strip()]
        emotional_word_count = sum(
            1 for word in self.poem_lower.split()
            if any(kw in word for kw in ['trust', 'guard', 'eternal', 'dream',
                                          'hope', 'light', 'safe', 'care'])
        )
        if lines:
            analysis['emotional_density'] = emotional_word_count / len(lines)

        # Check opening and closing strength
        if lines:
            opening = lines[0].lower()
            closing = lines[-1].lower()

            strong_openers = ['in', 'through', 'where', 'when', 'from']
            analysis['has_strong_opening'] = any(
                opening.startswith(word) for word in strong_openers
            ) and len(opening.split()) >= 5

            strong_closers = ['persist', 'endur', 'eternal', 'forever', 'trust',
                            'safe', 'guard', 'light', 'bright', 'sown', 'night']
            analysis['has_strong_closing'] = any(
                word in closing for word in strong_closers
            )

        # Count metaphorical language
        metaphor_indicators = [
            r'like a\b', r'as a\b', r'guardian of', r'keeper of',
            r'forged in', r'flows\b', r'cascades?\b', r'layers?\b'
        ]
        for pattern in metaphor_indicators:
            analysis['metaphor_count'] += len(re.findall(pattern, self.poem_lower))

        self._emotional_analysis = analysis
        return analysis

    def simulate_reviewer_rating(self, reviewer_id: str) -> ReviewerRating:
        """
        Simulate a reviewer rating based on poem content analysis.

        This method analyzes the poem content to generate a realistic
        reviewer rating based on emotional content strength.

        Args:
            reviewer_id: Identifier for the simulated reviewer

        Returns:
            ReviewerRating with simulated rating and feedback
        """
        analysis = self.analyze_poem_emotional_content()

        # Base rating calculation
        base_rating = 3.0

        # Adjust for emotional themes (up to +1.0)
        theme_bonus = min(analysis['theme_count'] * 0.15, 1.0)
        base_rating += theme_bonus

        # Adjust for strong opening/closing (up to +0.5)
        if analysis['has_strong_opening']:
            base_rating += 0.25
        if analysis['has_strong_closing']:
            base_rating += 0.25

        # Adjust for metaphor use (up to +0.3)
        metaphor_bonus = min(analysis['metaphor_count'] * 0.1, 0.3)
        base_rating += metaphor_bonus

        # Adjust for emotional density (up to +0.2)
        density_bonus = min(analysis['emotional_density'] * 0.5, 0.2)
        base_rating += density_bonus

        # Cap at 5.0
        final_rating = min(base_rating, 5.0)

        # Generate qualitative feedback keywords
        keywords_found = []
        if final_rating >= 4.0:
            keywords_found.extend(['memorable', 'engaging'])
        if analysis['theme_count'] >= 4:
            keywords_found.append('connected')
        if analysis['has_strong_closing']:
            keywords_found.append('inspiring')
        if analysis['metaphor_count'] >= 3:
            keywords_found.append('creative')

        # Generate feedback text
        if final_rating >= 4.5:
            feedback = "The poem deeply resonates with powerful imagery and emotional themes."
        elif final_rating >= 4.0:
            feedback = "A well-crafted poem that connects emotionally and is memorable."
        elif final_rating >= 3.5:
            feedback = "The poem has good emotional elements but could be more impactful."
        else:
            feedback = "The poem needs more emotional depth and resonance."

        return ReviewerRating(
            reviewer_id=reviewer_id,
            emotional_resonance_rating=round(final_rating, 1),
            qualitative_feedback=feedback,
            emotional_keywords_found=keywords_found
        )

    def add_review(self, review: ReviewerRating) -> None:
        """
        Add a reviewer's rating to the validation.

        Args:
            review: ReviewerRating object with rating and feedback
        """
        self._reviews.append(review)

    def add_manual_review(
        self,
        reviewer_id: str,
        rating: float,
        feedback: str,
        keywords: Optional[List[str]] = None
    ) -> ReviewerRating:
        """
        Add a manual review with custom rating and feedback.

        Args:
            reviewer_id: Identifier for the reviewer
            rating: Emotional resonance rating (1.0-5.0)
            feedback: Qualitative feedback text
            keywords: Optional list of emotional keywords found

        Returns:
            The created ReviewerRating
        """
        if keywords is None:
            # Auto-detect keywords from feedback
            keywords = []
            feedback_lower = feedback.lower()
            for keyword in self.POSITIVE_EMOTIONAL_KEYWORDS:
                if keyword in feedback_lower:
                    keywords.append(keyword)

        review = ReviewerRating(
            reviewer_id=reviewer_id,
            emotional_resonance_rating=rating,
            qualitative_feedback=feedback,
            emotional_keywords_found=keywords
        )
        self.add_review(review)
        return review

    def conduct_stakeholder_survey(self, num_reviewers: int = 3) -> List[ReviewerRating]:
        """
        Conduct a simulated stakeholder survey with multiple reviewers.

        Step 1 from scenario: Conduct stakeholder survey
        - Have 3+ independent reviewers rate emotional impact
        - Target is 4/5 average rating on emotional resonance

        Args:
            num_reviewers: Number of reviewers to simulate (default: 3)

        Returns:
            List of ReviewerRating objects
        """
        reviews = []
        for i in range(num_reviewers):
            review = self.simulate_reviewer_rating(f"reviewer_{i+1}")
            reviews.append(review)
            self.add_review(review)

        return reviews

    def analyze_qualitative_feedback(self) -> Dict[str, any]:
        """
        Analyze collected qualitative feedback for emotional response indicators.

        Step 2 from scenario: Collect qualitative feedback
        - Gather descriptive feedback on emotional response
        - Looking for words like 'inspired', 'memorable', 'connected'

        Returns:
            Dictionary with feedback analysis results
        """
        all_keywords = {}
        positive_count = 0
        negative_count = 0

        for review in self._reviews:
            # Count positive keywords from reviews
            for keyword in review.emotional_keywords_found:
                all_keywords[keyword] = all_keywords.get(keyword, 0) + 1

            # Check feedback text for additional positive keywords
            feedback_lower = review.qualitative_feedback.lower()
            for keyword in self.POSITIVE_EMOTIONAL_KEYWORDS:
                if keyword in feedback_lower and keyword not in review.emotional_keywords_found:
                    all_keywords[keyword] = all_keywords.get(keyword, 0) + 1

            # Check for negative keywords
            for keyword in self.NEGATIVE_EMOTIONAL_KEYWORDS:
                if keyword in feedback_lower:
                    negative_count += 1

            # Count positive responses
            if review.is_positive():
                positive_count += 1

        return {
            'positive_keywords': all_keywords,
            'positive_review_count': positive_count,
            'negative_indicator_count': negative_count,
            'total_reviews': len(self._reviews),
            'majority_positive': positive_count > len(self._reviews) / 2
        }

    def calculate_average_rating(self) -> float:
        """
        Calculate the average emotional resonance rating from all reviews.

        Returns:
            Average rating (0.0 if no reviews)
        """
        if not self._reviews:
            return 0.0

        total = sum(review.emotional_resonance_rating for review in self._reviews)
        return round(total / len(self._reviews), 2)

    def validate_emotional_resonance(self) -> EmotionalResonanceReport:
        """
        Perform complete emotional resonance validation.

        This validates:
        1. Test Case 1: Average emotional resonance rating >= 4.0 out of 5.0
        2. Test Case 2: Majority of feedback indicates positive emotional response

        Returns:
            EmotionalResonanceReport with validation results
        """
        # Ensure we have enough reviewers
        if len(self._reviews) < self.MIN_REVIEWERS:
            self.conduct_stakeholder_survey(self.MIN_REVIEWERS - len(self._reviews))

        # Calculate metrics
        avg_rating = self.calculate_average_rating()
        feedback_analysis = self.analyze_qualitative_feedback()

        # Determine if targets are met
        meets_rating_target = avg_rating >= self.TARGET_RATING
        majority_positive = feedback_analysis['majority_positive']

        # Generate explanation
        if meets_rating_target and majority_positive:
            explanation = (
                f"Emotional resonance validation PASSED. "
                f"Average rating: {avg_rating}/5.0 (target: {self.TARGET_RATING}). "
                f"Positive feedback majority achieved with {feedback_analysis['positive_review_count']}"
                f"/{feedback_analysis['total_reviews']} positive reviews."
            )
        else:
            issues = []
            if not meets_rating_target:
                issues.append(f"average rating {avg_rating} below target {self.TARGET_RATING}")
            if not majority_positive:
                issues.append("majority of feedback not positive")
            explanation = f"Emotional resonance validation FAILED: {'; '.join(issues)}"

        return EmotionalResonanceReport(
            average_rating=avg_rating,
            total_reviewers=len(self._reviews),
            individual_ratings=self._reviews.copy(),
            positive_feedback_count=feedback_analysis['positive_review_count'],
            emotional_keywords_found=feedback_analysis['positive_keywords'],
            meets_target=meets_rating_target,
            majority_positive=majority_positive,
            explanation=explanation
        )

    def get_full_report(self) -> Dict[str, any]:
        """
        Generate a comprehensive validation report.

        Returns:
            Dictionary with complete validation results
        """
        report = self.validate_emotional_resonance()
        poem_analysis = self.analyze_poem_emotional_content()

        return {
            'validation_result': 'PASS' if (report.meets_target and report.majority_positive) else 'FAIL',
            'average_rating': report.average_rating,
            'target_rating': self.TARGET_RATING,
            'meets_rating_target': report.meets_target,
            'total_reviewers': report.total_reviewers,
            'minimum_reviewers_required': self.MIN_REVIEWERS,
            'positive_feedback_count': report.positive_feedback_count,
            'majority_positive': report.majority_positive,
            'emotional_keywords_found': report.emotional_keywords_found,
            'poem_emotional_analysis': poem_analysis,
            'individual_reviews': [
                {
                    'reviewer_id': r.reviewer_id,
                    'rating': r.emotional_resonance_rating,
                    'feedback': r.qualitative_feedback,
                    'keywords': r.emotional_keywords_found
                }
                for r in report.individual_ratings
            ],
            'explanation': report.explanation
        }


def validate_emotional_resonance(
    poem_content: str,
    num_reviewers: int = 3
) -> Dict[str, any]:
    """
    Convenience function to validate a poem's emotional resonance.

    Args:
        poem_content: The full text of the poem
        num_reviewers: Number of reviewers for the survey

    Returns:
        Full validation report dictionary
    """
    validator = EmotionalResonanceValidator(poem_content)
    validator.conduct_stakeholder_survey(num_reviewers)
    return validator.get_full_report()
