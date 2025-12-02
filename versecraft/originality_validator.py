"""
Originality Validator Module for VerseCraft

This module provides functionality to validate that a poem is original and free
from copyright issues (REQ-8 from PRD). It checks for:
1. Plagiarism detection - verifying no significant overlap with known poems
2. Unique creation verification - confirming original composition for MirDB
"""

import re
import hashlib
from typing import List, Tuple, Dict, Optional


class OriginalityValidator:
    """
    Validates the originality of poems for legal use in marketing.

    This validator checks that poems are original compositions and not copied
    or adapted from existing copyrighted works.
    """

    # Known famous poem fragments that should not be copied
    KNOWN_POEM_FRAGMENTS = [
        "shall i compare thee to a summer's day",
        "two roads diverged in a yellow wood",
        "the road not taken",
        "do not go gentle into that good night",
        "rage, rage against the dying of the light",
        "i wandered lonely as a cloud",
        "hope is the thing with feathers",
        "because i could not stop for death",
        "the fog comes on little cat feet",
        "fire and ice",
        "to be, or not to be",
        "quoth the raven, nevermore",
        "once upon a midnight dreary",
        "in xanadu did kubla khan",
        "tyger tyger, burning bright",
        "how do i love thee? let me count the ways",
        "if you can keep your head when all about you",
        "i have measured out my life with coffee spoons",
        "april is the cruellest month",
        "i sing the body electric",
    ]

    # Technical product poem patterns that indicate adaptation from other products
    PRODUCT_POEM_PATTERNS = [
        r"in\s+circuits\s+deep.*(?:redis|mongodb|postgresql|mysql)",
        r"guardian\s+of\s+(?:data|bytes).*(?:redis|mongodb|cassandra)",
        r"keeper\s+of\s+(?:keys|records).*(?:redis|memcached|dynamodb)",
        r"forged\s+in\s+(?:java|python|golang|c\+\+).*(?:store|database)",
    ]

    # Minimum originality threshold (percentage of unique content)
    ORIGINALITY_THRESHOLD = 0.85

    # Maximum allowed n-gram overlap with known works
    MAX_NGRAM_OVERLAP = 3  # No more than 3 consecutive matching words

    def __init__(self, poem_content: str, product_name: str = "MirDB"):
        """
        Initialize the originality validator.

        Args:
            poem_content: The full text content of the poem to validate
            product_name: The product the poem is written for (default: MirDB)
        """
        self.poem_content = poem_content
        self.poem_lower = poem_content.lower()
        self.product_name = product_name.lower()
        self._validation_results: Dict[str, any] = {}

    def _normalize_text(self, text: str) -> str:
        """Normalize text for comparison by removing punctuation and extra spaces."""
        text = re.sub(r'[^\w\s]', '', text.lower())
        return ' '.join(text.split())

    def _extract_ngrams(self, text: str, n: int = 3) -> List[Tuple[str, ...]]:
        """Extract n-grams from text for plagiarism detection."""
        words = self._normalize_text(text).split()
        return [tuple(words[i:i+n]) for i in range(len(words) - n + 1)]

    def _compute_text_hash(self, text: str) -> str:
        """Compute a hash of normalized text for duplicate detection."""
        normalized = self._normalize_text(text)
        return hashlib.sha256(normalized.encode()).hexdigest()

    def check_known_poem_overlap(self) -> Tuple[bool, List[str]]:
        """
        Check if the poem contains significant overlap with known famous poems.

        Returns:
            Tuple of (is_original, list_of_found_overlaps)
        """
        found_overlaps = []
        normalized_poem = self._normalize_text(self.poem_content)

        for fragment in self.KNOWN_POEM_FRAGMENTS:
            if fragment in normalized_poem:
                found_overlaps.append(fragment)

        is_original = len(found_overlaps) == 0
        self._validation_results['known_poem_overlap'] = {
            'is_original': is_original,
            'found_overlaps': found_overlaps
        }
        return is_original, found_overlaps

    def check_product_poem_adaptation(self) -> Tuple[bool, List[str]]:
        """
        Check if the poem appears to be adapted from poems about other products.

        Returns:
            Tuple of (is_original, list_of_found_patterns)
        """
        found_patterns = []

        for pattern in self.PRODUCT_POEM_PATTERNS:
            if re.search(pattern, self.poem_lower):
                found_patterns.append(pattern)

        is_original = len(found_patterns) == 0
        self._validation_results['product_adaptation'] = {
            'is_original': is_original,
            'found_patterns': found_patterns
        }
        return is_original, found_patterns

    def verify_product_specificity(self) -> Tuple[bool, Dict[str, any]]:
        """
        Verify the poem is specifically written for the target product (MirDB).

        Returns:
            Tuple of (is_specific_to_product, details_dict)
        """
        details = {
            'product_mentioned': False,
            'technical_terms_found': [],
            'generic_score': 0,
            'specificity_score': 0
        }

        # Check for product name mention
        if self.product_name in self.poem_lower:
            details['product_mentioned'] = True
            details['specificity_score'] += 30

        # MirDB-specific technical terms
        mirdb_terms = [
            'lsm', 'sstable', 'memtable', 'skip list', 'skiplist',
            'compaction', 'write-ahead log', 'wal', 'rust',
            'key-value', 'keyvalue', 'memcached', 'persistence',
            'sorted string', 'levels', 'layer'
        ]

        for term in mirdb_terms:
            if term in self.poem_lower:
                details['technical_terms_found'].append(term)
                details['specificity_score'] += 10

        # Generic database/tech poem phrases that reduce specificity
        generic_phrases = [
            'any database', 'every server', 'all systems',
            'universal storage', 'generic cache'
        ]

        for phrase in generic_phrases:
            if phrase in self.poem_lower:
                details['generic_score'] += 20

        # Calculate final specificity
        final_score = details['specificity_score'] - details['generic_score']
        is_specific = (
            details['product_mentioned'] and
            len(details['technical_terms_found']) >= 2 and
            final_score >= 30
        )

        details['final_score'] = final_score
        self._validation_results['product_specificity'] = {
            'is_specific': is_specific,
            'details': details
        }
        return is_specific, details

    def check_ngram_uniqueness(self, reference_texts: Optional[List[str]] = None) -> Tuple[bool, Dict[str, any]]:
        """
        Check n-gram uniqueness to detect copied phrases.

        Args:
            reference_texts: Optional list of reference texts to check against

        Returns:
            Tuple of (is_unique, analysis_details)
        """
        poem_ngrams = self._extract_ngrams(self.poem_content, n=4)

        # Use known poem fragments as reference if none provided
        if reference_texts is None:
            reference_texts = self.KNOWN_POEM_FRAGMENTS

        all_reference_ngrams = set()
        for ref_text in reference_texts:
            ref_ngrams = self._extract_ngrams(ref_text, n=4)
            all_reference_ngrams.update(ref_ngrams)

        matching_ngrams = []
        for ngram in poem_ngrams:
            if ngram in all_reference_ngrams:
                matching_ngrams.append(' '.join(ngram))

        overlap_ratio = len(matching_ngrams) / max(len(poem_ngrams), 1)
        is_unique = overlap_ratio < (1 - self.ORIGINALITY_THRESHOLD)

        details = {
            'total_ngrams': len(poem_ngrams),
            'matching_ngrams': matching_ngrams,
            'overlap_ratio': overlap_ratio,
            'threshold': 1 - self.ORIGINALITY_THRESHOLD
        }

        self._validation_results['ngram_uniqueness'] = {
            'is_unique': is_unique,
            'details': details
        }
        return is_unique, details

    def verify_no_copyright_infringement(self) -> Tuple[bool, str]:
        """
        Comprehensive copyright infringement check.

        This method runs all plagiarism and originality checks.

        Returns:
            Tuple of (is_copyright_free, explanation)
        """
        checks = []

        # Run all checks
        known_overlap_ok, overlaps = self.check_known_poem_overlap()
        checks.append(('known_poem_overlap', known_overlap_ok, overlaps))

        product_adapt_ok, patterns = self.check_product_poem_adaptation()
        checks.append(('product_adaptation', product_adapt_ok, patterns))

        specificity_ok, spec_details = self.verify_product_specificity()
        checks.append(('product_specificity', specificity_ok, spec_details))

        ngram_ok, ngram_details = self.check_ngram_uniqueness()
        checks.append(('ngram_uniqueness', ngram_ok, ngram_details))

        # Aggregate results
        all_passed = all(check[1] for check in checks)

        if all_passed:
            explanation = (
                "Poem passes all originality checks: no known poem overlaps detected, "
                "no product adaptation patterns found, poem is specific to MirDB, "
                "and n-gram analysis confirms unique content."
            )
        else:
            failed_checks = [check[0] for check in checks if not check[1]]
            explanation = f"Originality concerns found in: {', '.join(failed_checks)}"

        self._validation_results['copyright_check'] = {
            'is_copyright_free': all_passed,
            'explanation': explanation,
            'checks_summary': {check[0]: check[1] for check in checks}
        }

        return all_passed, explanation

    def validate_unique_creation(self) -> Tuple[bool, str]:
        """
        Verify the poem is an original composition specifically for MirDB.

        This checks that:
        1. The poem is not adapted from poems about other products
        2. The poem contains MirDB-specific technical references
        3. The poem is uniquely crafted for this product

        Returns:
            Tuple of (is_unique_creation, explanation)
        """
        # Check for other product adaptations
        adaptation_ok, patterns = self.check_product_poem_adaptation()

        # Verify MirDB specificity
        specificity_ok, details = self.verify_product_specificity()

        is_unique = adaptation_ok and specificity_ok

        if is_unique:
            explanation = (
                f"Poem is uniquely created for {self.product_name.title()}: "
                f"Contains product-specific technical terms "
                f"({', '.join(details['technical_terms_found'][:3])}...), "
                f"mentions the product by name, and shows no adaptation from other products."
            )
        else:
            issues = []
            if not adaptation_ok:
                issues.append("appears adapted from other product poems")
            if not specificity_ok:
                issues.append("lacks product-specific content")
            explanation = f"Originality concerns: {'; '.join(issues)}"

        self._validation_results['unique_creation'] = {
            'is_unique': is_unique,
            'explanation': explanation
        }

        return is_unique, explanation

    def get_full_validation_report(self) -> Dict[str, any]:
        """
        Run all validations and return a comprehensive report.

        Returns:
            Dictionary containing all validation results and overall status
        """
        copyright_ok, copyright_explanation = self.verify_no_copyright_infringement()
        unique_ok, unique_explanation = self.validate_unique_creation()

        report = {
            'poem_hash': self._compute_text_hash(self.poem_content),
            'product': self.product_name,
            'overall_status': 'PASS' if (copyright_ok and unique_ok) else 'FAIL',
            'copyright_check': {
                'passed': copyright_ok,
                'explanation': copyright_explanation
            },
            'unique_creation_check': {
                'passed': unique_ok,
                'explanation': unique_explanation
            },
            'detailed_results': self._validation_results
        }

        return report


def validate_poem_originality(poem_content: str, product_name: str = "MirDB") -> Dict[str, any]:
    """
    Convenience function to validate a poem's originality.

    Args:
        poem_content: The full text of the poem
        product_name: The product the poem is for

    Returns:
        Full validation report dictionary
    """
    validator = OriginalityValidator(poem_content, product_name)
    return validator.get_full_validation_report()
