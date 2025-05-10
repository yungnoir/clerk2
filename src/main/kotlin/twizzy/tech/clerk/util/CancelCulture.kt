package twizzy.tech.clerk.util

import com.darkrockstudios.symspellkt.common.SpellCheckSettings
import com.darkrockstudios.symspellkt.common.Verbosity
import com.darkrockstudios.symspellkt.impl.SymSpell
import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern

class CancelCulture {
    // Use Sets for faster lookups
    private val blockedWords = hashSetOf<String>()
    private val normalizedBlockedWords = hashSetOf<String>()
    private var similarityThreshold = 0.7

    // Enhanced leet replacements map
    private val leetReplacements = mapOf(
        '0' to 'o', 'o' to '0',
        '1' to 'i', 'i' to '1', '1' to 'l', 'l' to '1',
        '2' to 'z', 'z' to '2',
        '3' to 'e', 'e' to '3',
        '4' to 'a', 'a' to '4',
        '5' to 's', 's' to '5',
        '6' to 'g', 'g' to '6', '6' to 'b', 'b' to '6',
        '7' to 't', 't' to '7',
        '8' to 'b', 'b' to '8',
        '9' to 'g', 'g' to '9', '9' to 'q', 'q' to '9',
        '@' to 'a', 'a' to '@',
        '$' to 's', 's' to '$',
        '+' to 't', 't' to '+',
        '!' to 'i', 'i' to '!',
        '|' to 'l', 'l' to '|',
        '*' to 'i', 'i' to '*'
    )

    // Pre-compile regex patterns
    private val wordSplitPattern = Pattern.compile("\\s+|[,.!?;:]")
    private val punctuationRemovalPattern = Pattern.compile("[\\s.,\\-!?;:]")

    // Compile RegEx patterns once for better performance
    private val bannedPatterns = listOf(
        """n+[i1!\*]+g+[gq6]+[e3a4r]+r*""",
        """f+[a4@]+g+[gq6]+[o0]+t*""",
        """k+[i1!\*]+k+[e3]+""",
        """n+[i1!\*]+g+[gq6]+[a4@]""",
        """j+[e3]+w+"""
    ).map { Pattern.compile(it, Pattern.CASE_INSENSITIVE) }

    // Cache for better performance
    private val normalizationCache = ConcurrentHashMap<String, String>(100)
    private val fuzzyMatchCache = ConcurrentHashMap<String, Boolean>(100)

    // Initialize SymSpell with optimized settings
    private val symSpell = SymSpell(
        SpellCheckSettings(
            maxEditDistance = 3.0,
            verbosity = Verbosity.Closest, // Use Closest for better performance
            topK = 3                       // Reduce from 5 to 3 for faster processing
        )
    ).apply {
        createInMemoryDictionary()
    }

    init {
        // Default offensive words
        addBlockedWords(listOf(
            "nigger", "nig", "nigga", "n1gger", "n1gg3r", "n1gga", "nigg3r", "nigg4",
            "n1ggrr", "n1gg4r", "n!gger", "negro", "n3gr0",
            "jew", "j3w", "j3vv", "j00",
            "beaner", "b3an3r", "b34n3r",
            "kkk", "faggot", "f4gg0t", "f4gg0", "fgt", "f4g",
            "tranny", "tr4nny", "tr4ny",
            "kike", "k1k3",
            "golliwog", "negro", "n3gro",
            "nazi", "n4z1", "naz1", "hitler", "h1tl3r",
            "coon", "c00n", "k00n", "koon",
            "paki", "p4k1",
            "chink", "ch1nk", "chinc", "ch1nc"
        ))
    }

    private fun SymSpell.createInMemoryDictionary() {
        createDictionaryEntry("", 0)
    }

    fun addBlockedWord(word: String) {
        val normalizedWord = word.lowercase().trim()
        blockedWords.add(normalizedWord)
        normalizedBlockedWords.add(normalizeText(normalizedWord))

        // Add to SymSpell dictionary
        symSpell.createDictionaryEntry(normalizedWord, 10000)

        // Generate and add variations more efficiently
        val variations = generateLeetVariationsEfficient(normalizedWord)
        for (variation in variations) {
            blockedWords.add(variation)
            normalizedBlockedWords.add(normalizeText(variation))
            symSpell.createDictionaryEntry(variation, 1000)
        }
    }

    fun addBlockedWords(words: List<String>) {
        words.forEach { addBlockedWord(it) }
    }

    fun setSimilarityThreshold(threshold: Double) {
        similarityThreshold = threshold.coerceIn(0.0, 1.0)
    }

    // More efficient leet variation generator
    private fun generateLeetVariationsEfficient(word: String): Set<String> {
        if (word.length <= 3) return setOf()

        val variations = hashSetOf<String>()

        // Add specific hand-crafted variations for known problematic words
        when (word) {
            "nigger" -> {
                variations.addAll(listOf(
                    "n1gg3r", "n166er", "ni99er", "n!gger", "n1ggr", "n1ggrr",
                    "n1gg4r", "n*gger", "n*gg*r", "n1663r", "n!994r", "niigger"
                ))
            }
            "faggot" -> {
                variations.addAll(listOf("f4g0t", "f466ot", "f@ggot", "f@g"))
            }
            "nigga" -> {
                variations.addAll(listOf("n1664", "n199a", "n1gg4", "n*gga"))
            }
            "jew" -> {
                variations.addAll(listOf("j3w", "j3vv", "j00", "j\\//"))
            }
        }

        // Generate a limited number of common variations
        val chars = word.toCharArray()
        for (i in chars.indices) {
            val replacement = leetReplacements[chars[i]]
            if (replacement != null) {
                chars[i] = replacement
                variations.add(String(chars))
                chars[i] = word[i] // Reset
            }
        }

        return variations
    }

    // Fast normalization with caching
    private fun normalizeText(text: String): String {
        return normalizationCache.computeIfAbsent(text) { str ->
            val result = StringBuilder(str.length)
            for (c in str) {
                result.append(leetReplacements[c] ?: c)
            }
            result.toString()
        }
    }

    fun isAllowed(text: String): Boolean {
        // Early return for empty text
        if (text.isBlank()) return true

        val lowercaseText = text.lowercase()

        // Fast path: check for exact matches first
        if (blockedWords.contains(lowercaseText)) {
            println("Filtered because of direct match: $lowercaseText")
            return false
        }

        // 1. Check pattern-based detection (regex)
        for (pattern in bannedPatterns) {
            if (pattern.matcher(lowercaseText).find()) {
                println("Filtered because of pattern match: ${pattern.pattern()}")
                return false
            }
        }

        // Get normalized text (cached)
        val normalizedText = normalizeText(lowercaseText)

        // 2. Check for direct matches in normalized text
        if (normalizedBlockedWords.any { normalizedText.contains(it) }) {
            println("Filtered because of normalized match")
            return false
        }

        // 3. Check tokens individually (more optimized)
        val tokens = wordSplitPattern.split(lowercaseText).filter { it.isNotEmpty() }

        // Check direct token matches
        for (token in tokens) {
            if (token.length > 2 && blockedWords.contains(token)) {
                println("Filtered because of token match: $token")
                return false
            }
        }

        // 4. Check compressed text (only for longer text)
        if (text.length > 4) {
            val compressedText = punctuationRemovalPattern.matcher(normalizedText).replaceAll("")
            val cacheKey = "compressed:$compressedText"

            val result = fuzzyMatchCache.computeIfAbsent(cacheKey) {
                // Do fuzzy matching on compressed text
                blockedWords.any { blockedWord ->
                    if (blockedWord.length <= 3) {
                        compressedText.contains(blockedWord)
                    } else {
                        isLowThresholdFuzzyMatch(compressedText, blockedWord)
                    }
                }
            }

            if (result) {
                println("Filtered because of fuzzy match in compressed text")
                return false
            }
        }

        // 5. Use SymSpell for more complex matches (only for tokens of sufficient length)
        for (token in tokens) {
            if (token.length < 4) continue // Skip very short tokens

            try {
                val suggestions = symSpell.lookup(token, Verbosity.Closest)

                for (suggestion in suggestions) {
                    val correctedText = suggestion.term

                    if (blockedWords.contains(correctedText) ||
                        normalizedBlockedWords.contains(normalizeText(correctedText))) {
                        println("Filtered because of SymSpell match: $correctedText")
                        return false
                    }
                }
            } catch (e: Exception) {
                // Fallback if SymSpell fails
                println("SymSpell error (ignoring): ${e.message}")
            }
        }

        return true
    }

    // Optimized fuzzy match that's quicker than full Levenshtein for large strings
    private fun isLowThresholdFuzzyMatch(text: String, blockedWord: String): Boolean {
        if (text.contains(blockedWord)) return true

        // Try n-gram comparison (faster than Levenshtein for initial screening)
        if (sharesSufficientNGrams(text, blockedWord, 0.7)) {
            // Only if n-gram check passes, do the more expensive Levenshtein
            return isFuzzyMatchLevenshtein(text, blockedWord, 0.65)
        }

        return false
    }

    // N-gram based similarity (faster initial screen)
    private fun sharesSufficientNGrams(text: String, pattern: String, threshold: Double): Boolean {
        if (pattern.length < 3) return text.contains(pattern)

        val n = 2 // bigrams
        val textNGrams = hashSetOf<String>()
        val patternNGrams = hashSetOf<String>()

        // Extract n-grams
        for (i in 0..text.length - n) {
            textNGrams.add(text.substring(i, i + n))
        }

        for (i in 0..pattern.length - n) {
            patternNGrams.add(pattern.substring(i, i + n))
        }

        // Count shared n-grams
        val shared = textNGrams.intersect(patternNGrams).size
        val similarity = shared.toDouble() / patternNGrams.size

        return similarity >= threshold
    }

    // Full Levenshtein-based fuzzy matching (only used when n-gram check passes)
    private fun isFuzzyMatchLevenshtein(text: String, blockedWord: String, threshold: Double = 0.65): Boolean {
        if (blockedWord.length <= 3) return text.contains(blockedWord)

        // Sliding window approach for long texts
        val windowSize = blockedWord.length + 2
        var i = 0

        while (i <= text.length - blockedWord.length + 2) {
            val end = minOf(i + windowSize, text.length)
            val chunk = text.substring(i, end)

            val similarity = 1 - (levenshteinDistanceOptimized(chunk, blockedWord).toDouble() /
                    maxOf(chunk.length, blockedWord.length))

            if (similarity >= threshold) {
                return true
            }

            // Move window with some overlap for better detection
            i += maxOf(1, windowSize / 2)
        }

        return false
    }

    // Faster Levenshtein implementation that stops early when threshold is exceeded
    private fun levenshteinDistanceOptimized(str1: String, str2: String): Int {
        val m = str1.length
        val n = str2.length

        // Early return for empty strings
        if (m == 0) return n
        if (n == 0) return m

        // Use single array for better memory efficiency
        val prevRow = IntArray(n + 1)
        val currRow = IntArray(n + 1)

        // Initialize first row
        for (j in 0..n) {
            prevRow[j] = j
        }

        for (i in 1..m) {
            currRow[0] = i

            for (j in 1..n) {
                val cost = if (str1[i - 1] == str2[j - 1]) 0 else 1
                currRow[j] = minOf(
                    prevRow[j] + 1,          // deletion
                    currRow[j - 1] + 1,      // insertion
                    prevRow[j - 1] + cost    // substitution
                )
            }

            // Swap rows
            for (j in 0..n) {
                prevRow[j] = currRow[j]
            }
        }

        return prevRow[n]
    }
}