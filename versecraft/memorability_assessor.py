"""
Memorability Assessment Module for VerseCraft

This module evaluates poem memorability through analysis of:
- Rhyme scheme (NFR-5): Consistent patterns that aid recall
- Repetition: Repeated phrases, refrains, and structural patterns
- Quotability: Standout lines that are memorable and quotable

Requirements Reference:
- NFR-5: Poem should be easily memorizable (use of rhyme, repetition)
- Story 4: Conference presenter needs memorable content
"""

import re
from typing import List, Tuple, Dict, Optional, Set
from dataclasses import dataclass
from collections import Counter


@dataclass
class RhymeAnalysis:
    """Results of rhyme scheme analysis."""
    scheme: str  # e.g., "AABB", "ABAB", "ABCB"
    rhyming_pairs: List[Tuple[str, str]]
    consistency_score: float  # 0.0 to 1.0
    aids_memorability: bool


@dataclass
class RepetitionAnalysis:
    """Results of repetition pattern analysis."""
    repeated_phrases: List[Tuple[str, int]]  # (phrase, count)
    refrains: List[str]  # Lines that appear multiple times
    structural_patterns: List[str]  # Anaphora, epistrophe, etc.
    has_memorable_repetition: bool


@dataclass
class QuotabilityAnalysis:
    """Results of quotability assessment."""
    quotable_lines: List[Tuple[str, float]]  # (line, score)
    standout_count: int
    meets_minimum: bool  # At least 2-3 standout lines


@dataclass
class MemorabilityReport:
    """Comprehensive memorability assessment report."""
    rhyme_analysis: RhymeAnalysis
    repetition_analysis: RepetitionAnalysis
    quotability_analysis: QuotabilityAnalysis
    overall_score: float
    passes_assessment: bool
    summary: str


class PhonemeRhymeDetector:
    """
    Detects rhymes using phonetic endings.

    Uses simplified phonetic rules based on common English spelling patterns,
    inspired by CMU Pronouncing Dictionary approaches but without external deps.
    """

    # Common rhyme endings grouped by sound
    RHYME_GROUPS = {
        'ay': ['ay', 'ey', 'eigh', 'ai', 'a-e'],
        'ee': ['ee', 'ea', 'ie', 'y', 'i', 'ey'],
        'ow': ['ow', 'ou', 'ough'],
        'oo': ['oo', 'ue', 'ew', 'o-e', 'ough'],
        'igh': ['igh', 'ie', 'y', 'i-e', 'eye'],
        'ore': ['ore', 'oar', 'oor', 'our', 'aw', 'or'],
        'air': ['air', 'are', 'ear', 'ere'],
        'ear': ['ear', 'eer', 'ere', 'ier'],
        'ight': ['ight', 'ite', 'yte'],
        'ound': ['ound', 'owned'],
        'tion': ['tion', 'sion'],
    }

    # Common ending sounds that indicate rhyme
    RHYME_ENDINGS = {
        # ast/ast sounds
        ('ast', 'ast'): True, ('ast', 'assed'): True,
        # eep/eep sounds
        ('eep', 'eep'): True, ('eep', 'eepe'): True,
        # ore/ore sounds
        ('ore', 'ore'): True, ('ore', 'oar'): True, ('ore', 'oor'): True,
        # ight/ight sounds
        ('ight', 'ight'): True, ('ight', 'ite'): True,
        # ace/ace sounds
        ('ace', 'ace'): True,
        # ue/oo sounds
        ('ue', 'ough'): True, ('ue', 'oo'): True, ('ue', 'ew'): True,
        ('oo', 'ue'): True, ('oo', 'ew'): True, ('oo', 'ough'): True,
        ('ough', 'ue'): True, ('ough', 'oo'): True,
        # one/un sounds
        ('one', 'un'): True, ('un', 'one'): True,
    }

    @classmethod
    def get_rhyme_ending(cls, word: str) -> str:
        """
        Extract the rhyming portion of a word (from last stressed vowel onward).

        Args:
            word: The word to analyze

        Returns:
            The phonetic ending for rhyme comparison
        """
        word = word.lower().strip()
        word = re.sub(r'[^a-z]', '', word)

        if not word:
            return ''

        # Find the last vowel cluster and everything after
        vowels = 'aeiouy'

        # Work backwards from the end to find vowel pattern
        last_vowel_pos = -1
        for i in range(len(word) - 1, -1, -1):
            if word[i] in vowels:
                last_vowel_pos = i
                # Continue back to find start of vowel cluster
                while last_vowel_pos > 0 and word[last_vowel_pos - 1] in vowels:
                    last_vowel_pos -= 1
                break

        if last_vowel_pos == -1:
            # No vowel found, use last 2 chars
            return word[-2:] if len(word) >= 2 else word

        # Get ending from last vowel cluster
        ending = word[last_vowel_pos:]

        return ending

    @classmethod
    def words_rhyme(cls, word1: str, word2: str) -> bool:
        """
        Determine if two words rhyme.

        Args:
            word1: First word
            word2: Second word

        Returns:
            True if the words rhyme
        """
        if not word1 or not word2:
            return False

        # Same word doesn't count as rhyme
        if word1.lower().strip() == word2.lower().strip():
            return False

        ending1 = cls.get_rhyme_ending(word1)
        ending2 = cls.get_rhyme_ending(word2)

        if not ending1 or not ending2:
            return False

        # Direct match
        if ending1 == ending2:
            return True

        # Check if endings are at least 2 chars and share suffix
        min_match = 2
        if len(ending1) >= min_match and len(ending2) >= min_match:
            # Match last 2+ characters
            for match_len in range(min(len(ending1), len(ending2)), min_match - 1, -1):
                if ending1[-match_len:] == ending2[-match_len:]:
                    return True

        # Check known rhyme endings
        if (ending1, ending2) in cls.RHYME_ENDINGS or (ending2, ending1) in cls.RHYME_ENDINGS:
            return True

        # Check rhyme group membership
        for group_endings in cls.RHYME_GROUPS.values():
            if ending1 in group_endings and ending2 in group_endings:
                return True

        return False


class RhymeSchemeAnalyzer:
    """
    Analyzes rhyme scheme patterns in poetry.

    Supports common schemes: AABB, ABAB, ABCB, ABBA, etc.
    """

    COMMON_SCHEMES = {
        'AABB': 'Couplet rhyme - easy to remember',
        'ABAB': 'Alternate rhyme - creates flow',
        'ABCB': 'Simple ballad - folk poetry style',
        'ABBA': 'Envelope rhyme - enclosing pattern',
        'AABA': 'Common in songs and folk',
        'AAAA': 'Monorhyme - strong emphasis',
    }

    def __init__(self, poem_content: str):
        """
        Initialize with poem content.

        Args:
            poem_content: Full text of the poem
        """
        self.poem_content = poem_content
        self.lines = self._extract_lines()
        self.stanzas = self._extract_stanzas()

    def _extract_lines(self) -> List[str]:
        """Extract non-empty content lines."""
        all_lines = self.poem_content.strip().split('\n')
        return [
            line.strip() for line in all_lines
            if line.strip() and not line.strip().startswith('#')
        ]

    def _extract_stanzas(self) -> List[List[str]]:
        """Split poem into stanzas (separated by blank lines)."""
        stanzas = []
        current_stanza = []

        for line in self.poem_content.split('\n'):
            stripped = line.strip()
            if stripped and not stripped.startswith('#'):
                current_stanza.append(stripped)
            elif current_stanza:
                stanzas.append(current_stanza)
                current_stanza = []

        if current_stanza:
            stanzas.append(current_stanza)

        return stanzas

    def _get_end_word(self, line: str) -> str:
        """Get the last word of a line."""
        words = re.findall(r"[a-zA-Z']+", line)
        return words[-1] if words else ''

    def analyze_stanza_scheme(self, stanza: List[str]) -> Tuple[str, List[Tuple[str, str]]]:
        """
        Analyze rhyme scheme for a single stanza.

        Args:
            stanza: List of lines in the stanza

        Returns:
            Tuple of (scheme_string, list_of_rhyming_pairs)
        """
        if not stanza:
            return '', []

        end_words = [self._get_end_word(line) for line in stanza]
        n = len(end_words)

        # Assign rhyme labels
        labels = [''] * n
        current_label = 'A'
        rhyming_pairs = []

        for i in range(n):
            if labels[i]:  # Already labeled
                continue

            labels[i] = current_label

            # Find all lines that rhyme with this one
            for j in range(i + 1, n):
                if not labels[j] and PhonemeRhymeDetector.words_rhyme(end_words[i], end_words[j]):
                    labels[j] = current_label
                    rhyming_pairs.append((end_words[i], end_words[j]))

            current_label = chr(ord(current_label) + 1)

        return ''.join(labels), rhyming_pairs

    def analyze_full_poem(self) -> RhymeAnalysis:
        """
        Analyze the complete rhyme scheme of the poem.

        Returns:
            RhymeAnalysis with scheme details
        """
        all_pairs = []
        stanza_schemes = []

        for stanza in self.stanzas:
            scheme, pairs = self.analyze_stanza_scheme(stanza)
            stanza_schemes.append(scheme)
            all_pairs.extend(pairs)

        # Check scheme consistency
        if stanza_schemes:
            # Normalize schemes to same length for comparison
            normalized = []
            for scheme in stanza_schemes:
                # Relabel starting from A
                mapping = {}
                result = []
                current = 'A'
                for c in scheme:
                    if c not in mapping:
                        mapping[c] = current
                        current = chr(ord(current) + 1)
                    result.append(mapping[c])
                normalized.append(''.join(result))

            # Calculate consistency
            if len(normalized) > 1:
                most_common = Counter(normalized).most_common(1)[0]
                consistency = most_common[1] / len(normalized)
            else:
                consistency = 1.0

            dominant_scheme = Counter(normalized).most_common(1)[0][0]
        else:
            consistency = 0.0
            dominant_scheme = ''

        # Determine if it aids memorability
        # Rhyme helps memorability if:
        # 1. There are rhyming pairs
        # 2. The scheme is relatively consistent
        aids_memorability = len(all_pairs) >= 2 and consistency >= 0.4

        return RhymeAnalysis(
            scheme=dominant_scheme,
            rhyming_pairs=all_pairs,
            consistency_score=consistency,
            aids_memorability=aids_memorability
        )


class RepetitionAnalyzer:
    """
    Analyzes repetition patterns that aid memorability.

    Detects:
    - Repeated words and phrases
    - Refrains (repeated lines)
    - Anaphora (same word/phrase at start of lines)
    - Epistrophe (same word/phrase at end of lines)
    """

    MIN_PHRASE_LENGTH = 2  # Minimum words for phrase detection
    MIN_REPETITIONS = 2   # Minimum occurrences to count as repetition

    def __init__(self, poem_content: str):
        """
        Initialize with poem content.

        Args:
            poem_content: Full text of the poem
        """
        self.poem_content = poem_content
        self.poem_lower = poem_content.lower()
        self.lines = self._extract_lines()

    def _extract_lines(self) -> List[str]:
        """Extract non-empty content lines."""
        all_lines = self.poem_content.strip().split('\n')
        return [
            line.strip() for line in all_lines
            if line.strip() and not line.strip().startswith('#')
        ]

    def find_repeated_phrases(self, min_length: int = 2) -> List[Tuple[str, int]]:
        """
        Find phrases that appear multiple times.

        Args:
            min_length: Minimum number of words in phrase

        Returns:
            List of (phrase, count) tuples
        """
        words = re.findall(r"[a-zA-Z']+", self.poem_lower)
        phrase_counts = Counter()

        # Check n-grams of various lengths
        for n in range(min_length, min(6, len(words))):
            for i in range(len(words) - n + 1):
                phrase = ' '.join(words[i:i + n])
                phrase_counts[phrase] += 1

        # Filter to phrases that appear multiple times
        repeated = [
            (phrase, count)
            for phrase, count in phrase_counts.items()
            if count >= self.MIN_REPETITIONS
        ]

        # Sort by significance (count * length)
        repeated.sort(key=lambda x: x[1] * len(x[0].split()), reverse=True)

        return repeated[:10]  # Top 10 most significant

    def find_refrains(self) -> List[str]:
        """
        Find lines that appear multiple times (refrains).

        Returns:
            List of refrain lines
        """
        line_counts = Counter(line.lower().strip() for line in self.lines)
        return [line for line, count in line_counts.items() if count >= 2]

    def detect_anaphora(self) -> List[Tuple[str, int]]:
        """
        Detect anaphora (same word/phrase at start of lines).

        Returns:
            List of (starting_phrase, count) tuples
        """
        starts = []
        for line in self.lines:
            words = re.findall(r"[a-zA-Z']+", line.lower())
            if words:
                # Get first 1-3 words
                for n in range(1, min(4, len(words) + 1)):
                    starts.append(' '.join(words[:n]))

        start_counts = Counter(starts)
        return [
            (phrase, count)
            for phrase, count in start_counts.items()
            if count >= 2 and len(phrase.split()) >= 1
        ]

    def detect_epistrophe(self) -> List[Tuple[str, int]]:
        """
        Detect epistrophe (same word/phrase at end of lines).

        Returns:
            List of (ending_phrase, count) tuples
        """
        ends = []
        for line in self.lines:
            words = re.findall(r"[a-zA-Z']+", line.lower())
            if words:
                # Get last 1-3 words
                for n in range(1, min(4, len(words) + 1)):
                    ends.append(' '.join(words[-n:]))

        end_counts = Counter(ends)
        return [
            (phrase, count)
            for phrase, count in end_counts.items()
            if count >= 2 and len(phrase.split()) >= 1
        ]

    def analyze(self) -> RepetitionAnalysis:
        """
        Perform full repetition analysis.

        Returns:
            RepetitionAnalysis with all findings
        """
        repeated_phrases = self.find_repeated_phrases()
        refrains = self.find_refrains()

        structural_patterns = []

        anaphora = self.detect_anaphora()
        if anaphora:
            top_anaphora = anaphora[0]
            if top_anaphora[1] >= 2:
                structural_patterns.append(f"Anaphora: '{top_anaphora[0]}' ({top_anaphora[1]}x)")

        epistrophe = self.detect_epistrophe()
        if epistrophe:
            top_epistrophe = epistrophe[0]
            if top_epistrophe[1] >= 2:
                structural_patterns.append(f"Epistrophe: '{top_epistrophe[0]}' ({top_epistrophe[1]}x)")

        # Determine if repetition aids memorability
        has_memorable_repetition = (
            len(repeated_phrases) >= 1 or
            len(refrains) >= 1 or
            len(structural_patterns) >= 1
        )

        return RepetitionAnalysis(
            repeated_phrases=repeated_phrases,
            refrains=refrains,
            structural_patterns=structural_patterns,
            has_memorable_repetition=has_memorable_repetition
        )


class QuotabilityAnalyzer:
    """
    Assesses which lines are most quotable and memorable.

    Criteria for quotability:
    - Conciseness (not too long)
    - Self-contained meaning
    - Vivid imagery or strong emotion
    - Universal appeal or insight
    - Rhythmic quality
    """

    # Words that indicate strong, quotable content
    STRONG_WORDS = {
        'never', 'always', 'forever', 'eternal', 'endless',
        'truth', 'light', 'dark', 'deep', 'rise', 'fall',
        'guard', 'guardian', 'keeper', 'forged', 'stand',
        'heart', 'soul', 'dream', 'fire', 'storm',
        'faithful', 'trust', 'endure', 'lasting', 'permanent',
        'transforms', 'transforming', 'cascade', 'flow'
    }

    # Structural patterns that aid quotability
    QUOTABLE_PATTERNS = [
        r'^[A-Z][^.!?]*[.!?]$',  # Complete sentence
        r'\b(never|always|forever)\b',  # Absolute terms
        r'\b(where|when|what)\b.*\b(there|then|that)\b',  # Parallel structure
    ]

    IDEAL_WORD_COUNT = (5, 12)  # Ideal length for quotable lines

    def __init__(self, poem_content: str):
        """
        Initialize with poem content.

        Args:
            poem_content: Full text of the poem
        """
        self.poem_content = poem_content
        self.lines = self._extract_lines()

    def _extract_lines(self) -> List[str]:
        """Extract non-empty content lines."""
        all_lines = self.poem_content.strip().split('\n')
        return [
            line.strip() for line in all_lines
            if line.strip() and not line.strip().startswith('#')
        ]

    def score_line(self, line: str) -> float:
        """
        Calculate quotability score for a single line.

        Args:
            line: The line to score

        Returns:
            Score from 0.0 to 1.0
        """
        score = 0.0
        words = re.findall(r"[a-zA-Z']+", line.lower())
        word_count = len(words)

        if word_count == 0:
            return 0.0

        # Length scoring (ideal is 5-12 words)
        if self.IDEAL_WORD_COUNT[0] <= word_count <= self.IDEAL_WORD_COUNT[1]:
            score += 0.25
        elif word_count < self.IDEAL_WORD_COUNT[0]:
            score += 0.1  # Too short
        else:
            score += 0.15  # A bit long but still quotable

        # Strong word bonus
        strong_count = sum(1 for w in words if w in self.STRONG_WORDS)
        score += min(0.3, strong_count * 0.1)

        # Pattern matching bonus
        for pattern in self.QUOTABLE_PATTERNS:
            if re.search(pattern, line, re.IGNORECASE):
                score += 0.1

        # Alliteration bonus
        if word_count >= 2:
            first_letters = [w[0].lower() for w in words if w]
            letter_counts = Counter(first_letters)
            max_alliteration = max(letter_counts.values())
            if max_alliteration >= 3:
                score += 0.15

        # Imagery indicators
        imagery_words = {'light', 'dark', 'deep', 'high', 'flow', 'fire', 'water', 'stone'}
        if any(w in imagery_words for w in words):
            score += 0.1

        # Position bonus (first and last lines often more memorable)
        # This will be applied in analyze()

        return min(1.0, score)

    def analyze(self, standout_threshold: float = 0.4) -> QuotabilityAnalysis:
        """
        Analyze quotability of all lines.

        Args:
            standout_threshold: Minimum score to be considered standout

        Returns:
            QuotabilityAnalysis with results
        """
        scored_lines = []

        for i, line in enumerate(self.lines):
            score = self.score_line(line)

            # Position bonus for first/last lines of poem or stanzas
            if i == 0 or i == len(self.lines) - 1:
                score = min(1.0, score + 0.1)

            scored_lines.append((line, score))

        # Sort by score descending
        scored_lines.sort(key=lambda x: x[1], reverse=True)

        # Count standout lines
        standout_count = sum(1 for _, score in scored_lines if score >= standout_threshold)

        # Get top quotable lines
        quotable_lines = [(line, score) for line, score in scored_lines if score >= standout_threshold]

        # Check if meets minimum requirement (2-3 standout lines)
        meets_minimum = standout_count >= 2

        return QuotabilityAnalysis(
            quotable_lines=quotable_lines[:10],  # Top 10
            standout_count=standout_count,
            meets_minimum=meets_minimum
        )


class MemorabilityAssessor:
    """
    Comprehensive memorability assessment combining all analyses.

    Evaluates poems for:
    - NFR-5: Easily memorizable through rhyme and repetition
    - Standout quotable lines
    """

    def __init__(self, poem_content: str):
        """
        Initialize with poem content.

        Args:
            poem_content: Full text of the poem
        """
        self.poem_content = poem_content
        self.rhyme_analyzer = RhymeSchemeAnalyzer(poem_content)
        self.repetition_analyzer = RepetitionAnalyzer(poem_content)
        self.quotability_analyzer = QuotabilityAnalyzer(poem_content)

    def assess(self) -> MemorabilityReport:
        """
        Perform complete memorability assessment.

        Returns:
            MemorabilityReport with all findings
        """
        rhyme = self.rhyme_analyzer.analyze_full_poem()
        repetition = self.repetition_analyzer.analyze()
        quotability = self.quotability_analyzer.analyze()

        # Calculate overall score (weighted)
        rhyme_weight = 0.4
        repetition_weight = 0.3
        quotability_weight = 0.3

        rhyme_score = rhyme.consistency_score if rhyme.aids_memorability else rhyme.consistency_score * 0.5
        repetition_score = 1.0 if repetition.has_memorable_repetition else 0.3
        quotability_score = min(1.0, quotability.standout_count / 3)  # 3+ standouts = full score

        overall_score = (
            rhyme_score * rhyme_weight +
            repetition_score * repetition_weight +
            quotability_score * quotability_weight
        )

        # Pass if at least 2 of 3 criteria are met
        criteria_met = sum([
            rhyme.aids_memorability,
            repetition.has_memorable_repetition,
            quotability.meets_minimum
        ])
        passes_assessment = criteria_met >= 2

        # Generate summary
        summary_parts = []

        if rhyme.aids_memorability:
            summary_parts.append(f"Rhyme scheme ({rhyme.scheme}) aids memorability")
        else:
            summary_parts.append("Limited rhyme scheme detected")

        if repetition.has_memorable_repetition:
            summary_parts.append("Contains memorable repeated elements")
        else:
            summary_parts.append("No significant repetition patterns found")

        if quotability.meets_minimum:
            summary_parts.append(f"{quotability.standout_count} standout quotable lines identified")
        else:
            summary_parts.append("Insufficient standout quotable lines")

        summary = "; ".join(summary_parts)

        return MemorabilityReport(
            rhyme_analysis=rhyme,
            repetition_analysis=repetition,
            quotability_analysis=quotability,
            overall_score=overall_score,
            passes_assessment=passes_assessment,
            summary=summary
        )


def assess_memorability(poem_content: str) -> MemorabilityReport:
    """
    Convenience function to assess poem memorability.

    Args:
        poem_content: Full text of the poem

    Returns:
        MemorabilityReport with complete assessment
    """
    assessor = MemorabilityAssessor(poem_content)
    return assessor.assess()
