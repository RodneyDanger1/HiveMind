//! Local semantic recall over persisted memory entries (no external services).
//!
//! # What this is, exactly
//!
//! This module implements a **lexical** recall fallback. It does **not** call an
//! embedding model, does **not** store vectors, and does **not** implement
//! approximate nearest neighbour search. "Semantic" here means: a token/tf-idf
//! inverted index plus a character-trigram fuzzy expansion, scored with a
//! cosine normalisation, so a paraphrased query that shares stems (and
//! near-miss spellings) with a stored entry still ranks that entry first.
//!
//! ## Index
//!
//! 1. Every value is tokenized: lowercase ASCII alphanumeric runs, stopwords
//!    dropped, then a fixed suffix stemmer is applied (`-ational`, `-ation`,
//!    `-ating`, `-ated`, `-ing`, `-ies`, `-ed`, `-es`, `-ly`, `-s`), so
//!    `invalidated`, `invalidating` and `invalidation` all index as
//!    `invalidate`.
//! 2. Only the first [`MAX_INDEXED_CHARS`] characters and the first
//!    [`MAX_TOKENS_PER_ENTRY`] tokens of a value are indexed. The hit excerpt is
//!    the first [`EXCERPT_CHARS`] characters of the full value.
//! 3. The inverted index maps each stem to its postings `(document, term
//!    frequency)`. Each term also contributes its padded character trigrams
//!    (`^term$`) to a trigram index, used only for fuzzy query expansion.
//! 4. Rows whose `trust` is `quarantined` are excluded, exactly like the
//!    existing `memory_read_set` filter does. They are counted in
//!    [`RecallResponse::quarantined_excluded`] but never returned.
//!
//! ## Scoring (exact formula)
//!
//! For a document `d` and term `t`:
//!
//! - `tf_weight(t, d) = (1 + ln tf(t, d)) * idf(t)` with `idf(t) = ln(1 + N/df(t))`
//!   and `N` = number of indexed documents.
//! - `doc_norm(d) = sqrt(sum_t tf_weight(t, d)^2)`.
//! - Query expansion: each query stem that exists in the dictionary contributes
//!   `1.0` to the query vector. A query stem that is **not** in the dictionary
//!   is fuzzily expanded to dictionary terms whose trigram Dice coefficient is
//!   `>= 0.5`; each contributes `0.5 * dice` (at most
//!   [`MAX_FUZZY_TERMS_PER_TOKEN`] terms per query token).
//! - `query_norm = sqrt(sum_t q_weight(t)^2)`.
//! - `score = sum_t q_weight(t) * tf_weight(t, d) / (query_norm * doc_norm(d))`,
//!   clamped to `0.0..=1.0`.
//!
//! Hits below [`RecallQuery::min_score`] are dropped; the rest are ordered by
//! score descending, then entry id ascending, so equal scores are deterministic.
//!
//! ## Provenance
//!
//! Every hit carries [`RecallProvenance`]: the row's `source_kind`, its raw
//! `trust` label (or `None`), an explicit [`Verification`] grade, and a
//! `verified: bool` flag. Only `verified`/`authoritative` rows have
//! `verified == true`. An absent trust label is `Unknown`, never "verified by
//! default". Recall never mutates trust, weights, code, or any row.
//!
//! ## Refusals
//!
//! An empty store is refused by name ([`RecallError::EmptyStore`],
//! `E_RECALL_STORE_EMPTY`). A store that breaks a recall invariant (blank entry
//! id / bank / key, a non-`active` row, or a duplicate entry id) is refused by
//! name ([`RecallError::CorruptStore`], `E_RECALL_STORE_CORRUPT`) instead of
//! being silently skipped. Bound violations are `E_RECALL_INDEX_BOUND`.
//!
//! ## Lesson promotion
//!
//! Recall results are evidence, not instructions. [`promote_lesson`] is the
//! only promotion path in this module: it refuses unless the operator supplies
//! an explicit [`LessonPromotionAck`] **and** an immutable evaluation receipt
//! at `data_dir/evolution/eval/<candidate_id>/receipt.json` passes
//! [`crate::evolution_gate::verify_evaluation_receipt`] (shape, held-out set,
//! and candidate confidence lower bound above the baseline upper bound). The
//! returned [`PromotionReceipt`] always records `weights_changed: false` and
//! `code_changed: false`; nothing in this module can silently change weights or
//! code.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use hiverouter_contract::{Refusal, RunId, Timestamp};
use serde::Serialize;

use crate::evolution_gate::verify_evaluation_receipt;
use crate::store::{MemoryEntryRecord, RunStore};

/// Identifier of the scoring algorithm, carried on every response so a caller
/// never has to guess what produced a score.
pub const ALGORITHM: &str = "lexical-tfidf-cosine+char3gram-fuzzy; no embeddings; no ANN";

/// Hard ceiling on the number of persisted entries one index may cover.
pub const MAX_INDEXED_ENTRIES: usize = 4096;
/// Hard ceiling on distinct terms in one index.
pub const MAX_TERMS: usize = 131_072;
/// Hard ceiling on postings in one index.
pub const MAX_POSTINGS: usize = 2_000_000;
/// Hard ceiling on the bytes of one indexed value.
pub const MAX_ENTRY_VALUE_BYTES: usize = 64 * 1024;
/// Characters of a value that are tokenized.
pub const MAX_INDEXED_CHARS: usize = 8192;
/// Tokens per entry that are indexed.
pub const MAX_TOKENS_PER_ENTRY: usize = 512;
/// Maximum query bytes.
pub const MAX_QUERY_BYTES: usize = 4096;
/// Maximum query tokens.
pub const MAX_QUERY_TOKENS: usize = 64;
/// Maximum hits a single recall may return.
pub const MAX_HITS: usize = 64;
/// Maximum banks one run-scoped recall may scan.
pub const MAX_BANKS_PER_RUN: usize = 64;
/// Maximum entries loaded from one bank before recall refuses.
pub const MAX_BANK_ENTRIES: usize = 1024;
/// Characters of a value returned as the hit excerpt.
pub const EXCERPT_CHARS: usize = 240;
/// Minimum trigram Dice coefficient for a fuzzy term match.
pub const FUZZY_MIN_DICE: f64 = 0.5;
/// Weight multiplier for a fuzzy query-term expansion.
pub const FUZZY_WEIGHT: f64 = 0.5;
/// Default minimum cosine score for a hit.
pub const DEFAULT_MIN_SCORE: f64 = 0.02;
/// Default hit limit.
pub const DEFAULT_LIMIT: usize = 8;
/// Maximum fuzzy dictionary terms considered per unknown query token.
pub const MAX_FUZZY_TERMS_PER_TOKEN: usize = 16;
/// Maximum candidate terms gathered from the trigram index per query token.
pub const MAX_FUZZY_CANDIDATES: usize = 512;

/// Words that carry no recall signal.
const STOPWORDS: &[&str] = &[
    "a", "about", "after", "all", "also", "an", "and", "any", "are", "as", "at", "be", "been",
    "before", "but", "by", "can", "could", "did", "do", "does", "for", "from", "had", "has",
    "have", "how", "if", "in", "into", "is", "it", "its", "may", "me", "might", "must", "my",
    "no", "not", "of", "on", "or", "our", "over", "should", "so", "some", "such", "than", "that",
    "the", "their", "then", "there", "these", "they", "this", "those", "to", "under", "up", "us",
    "use", "used", "using", "was", "we", "were", "what", "when", "where", "which", "who", "why",
    "will", "with", "would", "you", "your",
];

/// Deterministic suffix stems, longest first.
const STEM_SUFFIXES: &[&str] = &[
    "ational", "ation", "ating", "ated", "tion", "ing", "ies", "ed", "es", "ly", "s",
];

/// A named refusal for the recall feature. `code()` is the stable identity; the
/// message is human-facing detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecallError {
    /// The requested bank/run scope holds no readable memory entries.
    EmptyStore {
        /// The scope that was empty (bank id or `run:<id>`).
        scope: String,
    },
    /// A persisted row breaks an invariant that recall will not paper over.
    CorruptStore {
        /// The offending row's id, as persisted.
        entry_id: String,
        /// Why the row cannot be indexed.
        reason: &'static str,
    },
    /// The query has no usable token after stopword removal/stemming.
    EmptyQuery,
    /// The query exceeds [`MAX_QUERY_BYTES`] or [`MAX_QUERY_TOKENS`].
    QueryTooLong,
    /// The requested hit limit is outside `1..=MAX_HITS`.
    InvalidLimit {
        /// The limit the caller supplied.
        limit: usize,
    },
    /// The minimum score is not a finite value in `0.0..=1.0`.
    InvalidScore,
    /// A bounded index limit would be exceeded.
    IndexBound {
        /// Which bound was hit.
        reason: &'static str,
    },
    /// A named store refusal from the persistence layer.
    Store(Refusal),
}

impl RecallError {
    /// Stable refusal code. Empty/corrupt stores are distinguishable by name.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::EmptyStore { .. } => "E_RECALL_STORE_EMPTY",
            Self::CorruptStore { .. } => "E_RECALL_STORE_CORRUPT",
            Self::EmptyQuery => "E_RECALL_QUERY_EMPTY",
            Self::QueryTooLong => "E_RECALL_QUERY_TOO_LONG",
            Self::InvalidLimit { .. } => "E_RECALL_INVALID_LIMIT",
            Self::InvalidScore => "E_RECALL_INVALID_SCORE",
            Self::IndexBound { .. } => "E_RECALL_INDEX_BOUND",
            Self::Store(refusal) => refusal.code(),
        }
    }

    /// Human-readable cause, including the offending scope/row/bound.
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::EmptyStore { scope } => {
                format!("semantic recall found no readable memory entries in {scope}")
            }
            Self::CorruptStore { entry_id, reason } => {
                format!("semantic recall refused corrupt memory entry {entry_id}: {reason}")
            }
            Self::EmptyQuery => "semantic recall query has no usable tokens".to_owned(),
            Self::QueryTooLong => format!(
                "semantic recall query exceeds {MAX_QUERY_BYTES} bytes or {MAX_QUERY_TOKENS} tokens"
            ),
            Self::InvalidLimit { limit } => {
                format!("semantic recall limit {limit} is outside 1..={MAX_HITS}")
            }
            Self::InvalidScore => "semantic recall min_score must be finite and in 0.0..=1.0".to_owned(),
            Self::IndexBound { reason } => format!("semantic recall index bound: {reason}"),
            Self::Store(refusal) => refusal.message().to_owned(),
        }
    }
}

impl std::fmt::Display for RecallError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message())
    }
}

impl std::error::Error for RecallError {}

impl From<Refusal> for RecallError {
    fn from(refusal: Refusal) -> Self {
        Self::Store(refusal)
    }
}

/// Explicit verification grade of a recalled entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verification {
    /// Operator-authoritative row.
    Authoritative,
    /// Row promoted to `verified` by the operator path.
    Verified,
    /// Row explicitly labelled unverified by its writer.
    Unverified,
    /// Row has no trust label at all (for example a migration note).
    Unknown,
}

impl Verification {
    /// Maps the persisted trust label; absence is `Unknown`, never `Verified`.
    #[must_use]
    pub fn from_trust(trust: Option<&str>) -> Self {
        match trust {
            Some("authoritative") => Self::Authoritative,
            Some("verified") => Self::Verified,
            Some("unverified") => Self::Unverified,
            _ => Self::Unknown,
        }
    }

    /// True only for `verified`/`authoritative` rows.
    #[must_use]
    pub const fn is_verified(self) -> bool {
        matches!(self, Self::Verified | Self::Authoritative)
    }
}

/// Provenance carried by every hit: where the row came from and how far it can
/// be trusted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecallProvenance {
    /// Persisted `source_kind` (`agent_result`, `operator`, ...).
    pub source_kind: String,
    /// Raw trust label exactly as persisted; `None` when unlabelled.
    pub trust: Option<String>,
    /// Explicit verification grade.
    pub verification: Verification,
    /// Convenience flag derived from [`Self::verification`]; never independent.
    pub verified: bool,
}

/// One ranked recall hit.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RecallHit {
    /// Persisted entry id (the stable source id).
    pub entry_id: String,
    /// Owning memory bank id.
    pub bank_id: String,
    /// Stable entry key.
    pub key: String,
    /// Cosine score in `0.0..=1.0`.
    pub score: f64,
    /// Provenance/verification labels.
    pub provenance: RecallProvenance,
    /// Bounded excerpt of the stored value.
    pub excerpt: String,
}

/// Ranked recall result plus the exact query/index facts behind it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RecallResponse {
    /// Hits ordered by score descending, then entry id ascending.
    pub hits: Vec<RecallHit>,
    /// Stemmed query tokens (deduplicated, input order).
    pub query_terms: Vec<String>,
    /// Number of documents in the index.
    pub indexed_entries: usize,
    /// Quarantined rows excluded from the index.
    pub quarantined_excluded: usize,
    /// Algorithm identifier ([`ALGORITHM`]).
    pub algorithm: &'static str,
}

/// Bounded query parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RecallQuery<'a> {
    /// Query text.
    pub text: &'a str,
    /// Maximum hits, `1..=MAX_HITS`.
    pub limit: usize,
    /// Minimum score, finite and in `0.0..=1.0`.
    pub min_score: f64,
}

impl<'a> RecallQuery<'a> {
    /// Query with default limit and minimum score.
    #[must_use]
    pub const fn new(text: &'a str) -> Self {
        Self {
            text,
            limit: DEFAULT_LIMIT,
            min_score: DEFAULT_MIN_SCORE,
        }
    }

    /// Overrides the hit limit.
    #[must_use]
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Overrides the minimum score.
    #[must_use]
    pub const fn with_min_score(mut self, min_score: f64) -> Self {
        self.min_score = min_score;
        self
    }
}

#[derive(Debug, Clone)]
struct IndexedDocument {
    entry_id: String,
    bank_id: String,
    key: String,
    source_kind: String,
    trust: Option<String>,
    excerpt: String,
    norm: f64,
}

#[derive(Debug, Clone, Copy)]
struct TermPosting {
    document: u32,
    tf: u32,
}

/// The bounded lexical index.
#[derive(Debug)]
pub struct SemanticIndex {
    documents: Vec<IndexedDocument>,
    term_ids: HashMap<String, u32>,
    terms: Vec<String>,
    term_trigrams: Vec<Vec<[u8; 3]>>,
    postings: Vec<Vec<TermPosting>>,
    idf: Vec<f64>,
    trigram_terms: HashMap<[u8; 3], Vec<u32>>,
    quarantined_excluded: usize,
}

impl SemanticIndex {
    /// Builds the index over already-loaded persisted rows.
    ///
    /// # Errors
    /// [`RecallError::CorruptStore`] for a blank id/bank/key, a non-`active`
    /// status, or a duplicate entry id; [`RecallError::IndexBound`] when a
    /// bounded limit would be exceeded.
    pub fn build(entries: &[MemoryEntryRecord]) -> Result<Self, RecallError> {
        if entries.len() > MAX_INDEXED_ENTRIES {
            return Err(RecallError::IndexBound {
                reason: "entry count exceeds MAX_INDEXED_ENTRIES",
            });
        }
        let mut documents = Vec::with_capacity(entries.len());
        let mut document_terms: Vec<BTreeMap<String, u32>> = Vec::with_capacity(entries.len());
        let mut term_document_tf: BTreeMap<String, Vec<(u32, u32)>> = BTreeMap::new();
        let mut seen_ids: HashSet<&str> = HashSet::with_capacity(entries.len());
        let mut quarantined_excluded = 0_usize;

        for entry in entries {
            validate_entry(entry)?;
            if !seen_ids.insert(entry.id.as_str()) {
                return Err(RecallError::CorruptStore {
                    entry_id: entry.id.clone(),
                    reason: "duplicate entry id",
                });
            }
            if entry.trust.as_deref() == Some("quarantined") {
                quarantined_excluded += 1;
                continue;
            }
            let document_index =
                u32::try_from(documents.len()).map_err(|_| RecallError::IndexBound {
                    reason: "document index exceeds u32",
                })?;
            let mut counts: BTreeMap<String, u32> = BTreeMap::new();
            let mut token_count = 0_usize;
            for token in tokenize(&entry.value) {
                if token_count >= MAX_TOKENS_PER_ENTRY {
                    break;
                }
                token_count += 1;
                *counts.entry(stem(&token)).or_insert(0) += 1;
            }
            for (term, tf) in &counts {
                term_document_tf
                    .entry(term.clone())
                    .or_default()
                    .push((document_index, *tf));
            }
            document_terms.push(counts);
            documents.push(IndexedDocument {
                entry_id: entry.id.clone(),
                bank_id: entry.bank_id.clone(),
                key: entry.key.clone(),
                source_kind: entry.source_kind.clone(),
                trust: entry.trust.clone(),
                excerpt: excerpt_of(&entry.value),
                norm: 0.0,
            });
        }

        if term_document_tf.len() > MAX_TERMS {
            return Err(RecallError::IndexBound {
                reason: "distinct term count exceeds MAX_TERMS",
            });
        }
        let total_postings: usize = term_document_tf.values().map(Vec::len).sum();
        if total_postings > MAX_POSTINGS {
            return Err(RecallError::IndexBound {
                reason: "posting count exceeds MAX_POSTINGS",
            });
        }

        let document_count = documents.len() as f64;
        let mut term_ids = HashMap::with_capacity(term_document_tf.len());
        let mut terms = Vec::with_capacity(term_document_tf.len());
        let mut postings = Vec::with_capacity(term_document_tf.len());
        let mut idf = Vec::with_capacity(term_document_tf.len());
        let mut term_trigrams = Vec::with_capacity(term_document_tf.len());
        let mut trigram_terms: HashMap<[u8; 3], Vec<u32>> = HashMap::new();

        for (term, entries_for_term) in term_document_tf {
            let term_id = u32::try_from(terms.len()).map_err(|_| RecallError::IndexBound {
                reason: "term id exceeds u32",
            })?;
            let df = entries_for_term.len() as f64;
            let weight = (1.0 + document_count / df).ln();
            term_ids.insert(term.clone(), term_id);
            for trigram in trigrams(&term) {
                trigram_terms.entry(trigram).or_default().push(term_id);
            }
            term_trigrams.push(trigrams(&term));
            terms.push(term);
            idf.push(weight);
            postings.push(
                entries_for_term
                    .into_iter()
                    .map(|(document, tf)| TermPosting { document, tf })
                    .collect(),
            );
        }

        for (position, counts) in document_terms.iter().enumerate() {
            let mut sum = 0.0_f64;
            for (term, tf) in counts {
                let Some(term_id) = term_ids.get(term) else {
                    continue;
                };
                let index = *term_id as usize;
                let weight = (1.0 + f64::from(*tf)).ln() * idf[index];
                sum += weight * weight;
            }
            documents[position].norm = sum.sqrt();
        }

        Ok(Self {
            documents,
            term_ids,
            terms,
            term_trigrams,
            postings,
            idf,
            trigram_terms,
            quarantined_excluded,
        })
    }

    /// Number of indexed documents.
    #[must_use]
    pub fn indexed_entries(&self) -> usize {
        self.documents.len()
    }

    /// Ranks documents against a bounded query.
    ///
    /// # Errors
    /// [`RecallError::EmptyQuery`], [`RecallError::QueryTooLong`],
    /// [`RecallError::InvalidLimit`], or [`RecallError::InvalidScore`].
    pub fn recall(&self, query: &RecallQuery<'_>) -> Result<RecallResponse, RecallError> {
        if query.text.len() > MAX_QUERY_BYTES {
            return Err(RecallError::QueryTooLong);
        }
        if query.limit == 0 || query.limit > MAX_HITS {
            return Err(RecallError::InvalidLimit { limit: query.limit });
        }
        if !query.min_score.is_finite() || !(0.0..=1.0).contains(&query.min_score) {
            return Err(RecallError::InvalidScore);
        }

        let raw_tokens = tokenize(query.text);
        if raw_tokens.len() > MAX_QUERY_TOKENS {
            return Err(RecallError::QueryTooLong);
        }
        let mut query_terms: Vec<String> = Vec::with_capacity(raw_tokens.len());
        for token in raw_tokens {
            let stemmed = stem(&token);
            if !stemmed.is_empty() {
                query_terms.push(stemmed);
            }
        }
        if query_terms.is_empty() {
            return Err(RecallError::EmptyQuery);
        }
        let mut deduped = Vec::with_capacity(query_terms.len());
        for term in &query_terms {
            if !deduped.contains(term) {
                deduped.push(term.clone());
            }
        }

        let (query_vector, query_norm) = self.expand(&query_terms);
        let mut contributions: BTreeMap<u32, f64> = BTreeMap::new();
        if query_norm > 0.0 {
            for (term_id, query_weight) in &query_vector {
                let index = *term_id as usize;
                let Some(term_postings) = self.postings.get(index) else {
                    continue;
                };
                let idf = self.idf[index];
                for posting in term_postings {
                    let document_weight = (1.0 + f64::from(posting.tf)).ln() * idf;
                    *contributions.entry(posting.document).or_insert(0.0) +=
                        query_weight * document_weight;
                }
            }
        }

        let mut hits = Vec::new();
        for (document_index, numerator) in contributions {
            let Ok(index) = usize::try_from(document_index) else {
                continue;
            };
            let Some(document) = self.documents.get(index) else {
                continue;
            };
            if document.norm <= 0.0 {
                continue;
            }
            let score = (numerator / (query_norm * document.norm)).clamp(0.0, 1.0);
            if score < query.min_score {
                continue;
            }
            let verification = Verification::from_trust(document.trust.as_deref());
            hits.push(RecallHit {
                entry_id: document.entry_id.clone(),
                bank_id: document.bank_id.clone(),
                key: document.key.clone(),
                score,
                provenance: RecallProvenance {
                    source_kind: document.source_kind.clone(),
                    trust: document.trust.clone(),
                    verification,
                    verified: verification.is_verified(),
                },
                excerpt: document.excerpt.clone(),
            });
        }
        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.entry_id.cmp(&right.entry_id))
        });
        hits.truncate(query.limit);

        Ok(RecallResponse {
            hits,
            query_terms: deduped,
            indexed_entries: self.documents.len(),
            quarantined_excluded: self.quarantined_excluded,
            algorithm: ALGORITHM,
        })
    }

    fn expand(&self, query_terms: &[String]) -> (BTreeMap<u32, f64>, f64) {
        let mut vector: BTreeMap<u32, f64> = BTreeMap::new();
        for term in query_terms {
            if let Some(term_id) = self.term_ids.get(term) {
                *vector.entry(*term_id).or_insert(0.0) += 1.0;
                continue;
            }
            let query_trigrams = trigrams(term);
            let mut candidates: Vec<u32> = Vec::new();
            for trigram in &query_trigrams {
                if let Some(term_ids) = self.trigram_terms.get(trigram) {
                    candidates.extend_from_slice(term_ids);
                }
            }
            candidates.sort_unstable();
            candidates.dedup();
            candidates.truncate(MAX_FUZZY_CANDIDATES);
            let mut scored: Vec<(u32, f64)> = candidates
                .into_iter()
                .filter_map(|term_id| {
                    let index = term_id as usize;
                    let dice = dice_coefficient(&query_trigrams, self.term_trigrams.get(index)?);
                    (dice >= FUZZY_MIN_DICE).then_some((term_id, dice))
                })
                .collect();
            scored.sort_by(|left, right| {
                right
                    .1
                    .partial_cmp(&left.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| left.0.cmp(&right.0))
            });
            scored.truncate(MAX_FUZZY_TERMS_PER_TOKEN);
            for (term_id, dice) in scored {
                *vector.entry(term_id).or_insert(0.0) += FUZZY_WEIGHT * dice;
            }
        }
        let norm = vector.values().map(|weight| weight * weight).sum::<f64>().sqrt();
        (vector, norm)
    }
}

fn validate_entry(entry: &MemoryEntryRecord) -> Result<(), RecallError> {
    let corrupt = |reason: &'static str| RecallError::CorruptStore {
        entry_id: entry.id.clone(),
        reason,
    };
    if entry.id.trim().is_empty() {
        return Err(corrupt("blank entry id"));
    }
    if entry.bank_id.trim().is_empty() {
        return Err(corrupt("blank bank id"));
    }
    if entry.key.trim().is_empty() {
        return Err(corrupt("blank entry key"));
    }
    if entry.status != "active" {
        return Err(corrupt("entry is not active"));
    }
    if entry.value.len() > MAX_ENTRY_VALUE_BYTES {
        return Err(RecallError::IndexBound {
            reason: "entry value exceeds MAX_ENTRY_VALUE_BYTES",
        });
    }
    Ok(())
}

/// Lowercase ASCII alphanumeric tokens, stopwords dropped. The value is bounded
/// to [`MAX_INDEXED_CHARS`] characters before tokenizing.
fn tokenize(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for character in text.chars().take(MAX_INDEXED_CHARS) {
        if character.is_ascii_alphanumeric() {
            current.push(character.to_ascii_lowercase());
        } else if !current.is_empty() {
            push_token(&mut tokens, &mut current);
        }
        if tokens.len() >= MAX_TOKENS_PER_ENTRY {
            break;
        }
    }
    if !current.is_empty() && tokens.len() < MAX_TOKENS_PER_ENTRY {
        push_token(&mut tokens, &mut current);
    }
    tokens
}

fn push_token(tokens: &mut Vec<String>, current: &mut String) {
    let token = std::mem::take(current);
    if token.len() > 48 || STOPWORDS.contains(&token.as_str()) {
        return;
    }
    tokens.push(token);
}

/// Deterministic suffix stemmer; see the module docs for the exact rule.
fn stem(token: &str) -> String {
    let mut stemmed = token.to_owned();
    for suffix in STEM_SUFFIXES {
        if stemmed.len() <= suffix.len() + 3 {
            continue;
        }
        if !stemmed.ends_with(suffix) {
            continue;
        }
        match *suffix {
            "ies" => {
                stemmed.truncate(stemmed.len() - 3);
                stemmed.push('y');
            }
            "es" => {
                // Drop only the plural `s`: `caches` -> `cache`, `boxes` ->
                // `boxe`. Keeping one deterministic rule per suffix is worth
                // more here than English exception tables; near-singulars still
                // meet the trigram fuzzy threshold.
                stemmed.truncate(stemmed.len() - 1);
            }
            "ation" | "ating" | "ated" => {
                stemmed.truncate(stemmed.len() - suffix.len());
                stemmed.push_str("ate");
            }
            "s" => {
                if !stemmed.ends_with("ss") && !stemmed.ends_with("us") && !stemmed.ends_with("is") {
                    stemmed.truncate(stemmed.len() - 1);
                }
            }
            _ => stemmed.truncate(stemmed.len() - suffix.len()),
        }
        break;
    }
    stemmed
}

/// Padded character trigrams (`^term$`); terms are ASCII after tokenizing.
fn trigrams(term: &str) -> Vec<[u8; 3]> {
    let padded = format!("^{term}$");
    let bytes = padded.as_bytes();
    let mut grams = Vec::new();
    if bytes.len() < 3 {
        return grams;
    }
    for window in bytes.windows(3) {
        grams.push([window[0], window[1], window[2]]);
    }
    grams.sort_unstable();
    grams.dedup();
    grams
}

fn dice_coefficient(left: &[[u8; 3]], right: &[[u8; 3]]) -> f64 {
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    let mut intersection = 0_usize;
    let (mut left_index, mut right_index) = (0_usize, 0_usize);
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                intersection += 1;
                left_index += 1;
                right_index += 1;
            }
        }
    }
    let total = left.len() + right.len();
    if total == 0 {
        0.0
    } else {
        2.0 * intersection as f64 / total as f64
    }
}

fn excerpt_of(value: &str) -> String {
    let mut excerpt: String = value.chars().take(EXCERPT_CHARS).collect();
    if value.chars().count() > EXCERPT_CHARS {
        excerpt.push('…');
    }
    excerpt
}

/// Loads one run-scoped bank and recalls against it.
///
/// # Errors
/// [`RecallError::EmptyStore`] when the bank holds no active rows;
/// [`RecallError::Store`] for persistence refusals (including a bank that
/// belongs to another run); [`RecallError::IndexBound`] when the bank holds
/// more than [`MAX_BANK_ENTRIES`] entries; plus the build/recall refusals.
pub fn recall_bank(
    store: &RunStore,
    run_id: RunId,
    bank_id: &str,
    query: &RecallQuery<'_>,
) -> Result<RecallResponse, RecallError> {
    if bank_id.trim().is_empty() {
        return Err(RecallError::Store(Refusal::SchemaInvalid));
    }
    let mut entries = store.memory_entries_for_bank_scoped(run_id, bank_id, MAX_BANK_ENTRIES + 1)?;
    if entries.len() > MAX_BANK_ENTRIES {
        entries.clear();
        return Err(RecallError::IndexBound {
            reason: "bank entry count exceeds MAX_BANK_ENTRIES",
        });
    }
    if entries.is_empty() {
        return Err(RecallError::EmptyStore {
            scope: bank_id.to_owned(),
        });
    }
    SemanticIndex::build(&entries)?.recall(query)
}

/// Loads every open bank of one run and recalls across the merged set.
///
/// Bank isolation is preserved: only banks whose `run_id` is `run_id` are read,
/// and each read uses [`RunStore::memory_entries_for_bank_scoped`].
///
/// # Errors
/// [`RecallError::EmptyStore`] when the run has no readable entries;
/// [`RecallError::IndexBound`] when the run exceeds [`MAX_BANKS_PER_RUN`] open
/// banks or [`MAX_INDEXED_ENTRIES`] entries; plus the build/recall refusals.
pub fn recall_run(
    store: &RunStore,
    run_id: RunId,
    query: &RecallQuery<'_>,
) -> Result<RecallResponse, RecallError> {
    let banks = store.memory_banks_for_run(run_id)?;
    let open: Vec<_> = banks
        .into_iter()
        .filter(|bank| bank.closed_at.is_none())
        .collect();
    if open.is_empty() {
        return Err(RecallError::EmptyStore {
            scope: format!("run:{run_id}"),
        });
    }
    if open.len() > MAX_BANKS_PER_RUN {
        return Err(RecallError::IndexBound {
            reason: "open bank count exceeds MAX_BANKS_PER_RUN",
        });
    }
    let mut entries: Vec<MemoryEntryRecord> = Vec::new();
    for bank in &open {
        let mut bank_entries =
            store.memory_entries_for_bank_scoped(run_id, &bank.id, MAX_BANK_ENTRIES + 1)?;
        if bank_entries.len() > MAX_BANK_ENTRIES {
            bank_entries.clear();
            return Err(RecallError::IndexBound {
                reason: "bank entry count exceeds MAX_BANK_ENTRIES",
            });
        }
        entries.append(&mut bank_entries);
        if entries.len() > MAX_INDEXED_ENTRIES {
            return Err(RecallError::IndexBound {
                reason: "run entry count exceeds MAX_INDEXED_ENTRIES",
            });
        }
    }
    if entries.is_empty() {
        return Err(RecallError::EmptyStore {
            scope: format!("run:{run_id}"),
        });
    }
    SemanticIndex::build(&entries)?.recall(query)
}

/// Explicit operator acknowledgement required for a lesson promotion. There is
/// no `Default` and no other constructor: the operator id is mandatory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LessonPromotionAck {
    operator: String,
}

impl LessonPromotionAck {
    /// Builds the acknowledgement from a non-blank operator id (max 128 chars).
    ///
    /// # Errors
    /// [`Refusal::SchemaInvalid`] when the operator id is blank or oversized.
    pub fn explicit(operator: &str) -> Result<Self, Refusal> {
        let trimmed = operator.trim();
        if trimmed.is_empty() || trimmed.len() > 128 {
            return Err(Refusal::SchemaInvalid);
        }
        Ok(Self {
            operator: trimmed.to_owned(),
        })
    }

    /// The operator id that supplied the acknowledgement.
    #[must_use]
    pub fn operator(&self) -> &str {
        &self.operator
    }
}

/// Verified admission of one lesson. Not a weight change and not a code change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PromotionReceipt {
    /// Candidate/proposal id the receipt is bound to.
    pub candidate_id: String,
    /// Lesson area key.
    pub area: String,
    /// SHA-256 of the exact evaluation-receipt bytes.
    pub receipt_sha256: String,
    /// Held-out task-set id from the receipt.
    pub task_set_id: String,
    /// Held-out task count from the receipt.
    pub task_count: u64,
    /// Baseline pass rate from the receipt.
    pub baseline_pass_rate: String,
    /// Candidate pass rate from the receipt.
    pub candidate_pass_rate: String,
    /// Operator that acknowledged the promotion.
    pub operator: String,
    /// Promotion time (RFC3339 UTC).
    pub promoted_at: String,
    /// Always `false`: promotion never changes scoring weights.
    pub weights_changed: bool,
    /// Always `false`: promotion never changes code.
    pub code_changed: bool,
}

impl PromotionReceipt {
    /// Deterministic audit line. Contains no secret and no memory value.
    #[must_use]
    pub fn audit_line(&self) -> String {
        serde_json::json!({
            "state": "lesson_promotion_verified",
            "candidate_id": self.candidate_id,
            "area": self.area,
            "receipt_sha256": self.receipt_sha256,
            "task_set_id": self.task_set_id,
            "task_count": self.task_count,
            "baseline_pass_rate": self.baseline_pass_rate,
            "candidate_pass_rate": self.candidate_pass_rate,
            "operator": self.operator,
            "promoted_at": self.promoted_at,
            "weights_changed": self.weights_changed,
            "code_changed": self.code_changed,
        })
        .to_string()
    }
}

/// The only lesson-promotion path. Requires an explicit operator
/// acknowledgement **and** an immutable, verified evaluation receipt on disk.
/// It never writes weights, code, or the lesson ledger; it returns the verified
/// admission receipt the caller must present to the existing lesson writer.
///
/// # Errors
/// [`Refusal::SchemaInvalid`] for blank/oversized candidate text;
/// [`Refusal::ApprovalRequired`] when the receipt is absent;
/// [`Refusal::SchemaInvalid`] when the receipt is malformed, bound to another
/// proposal, weaker than the held-out minimum, or does not show the candidate
/// lower confidence bound above the baseline upper bound.
pub fn promote_lesson(
    data_dir: &Path,
    candidate_id: &str,
    area: &str,
    problem: &str,
    proposal: &str,
    ack: &LessonPromotionAck,
) -> Result<PromotionReceipt, Refusal> {
    let candidate_id = candidate_id.trim();
    let area = area.trim();
    let problem = problem.trim();
    let proposal = proposal.trim();
    if candidate_id.is_empty()
        || candidate_id.len() > 128
        || area.is_empty()
        || area.len() > 64
        || problem.is_empty()
        || problem.len() > 512
        || proposal.is_empty()
        || proposal.len() > 4096
    {
        return Err(Refusal::SchemaInvalid);
    }
    if ack.operator().is_empty() {
        return Err(Refusal::ApprovalRequired);
    }
    let verified = verify_evaluation_receipt(data_dir, candidate_id)?;
    Ok(PromotionReceipt {
        candidate_id: candidate_id.to_owned(),
        area: area.to_owned(),
        receipt_sha256: verified.sha256,
        task_set_id: verified.task_set_id,
        task_count: verified.task_count,
        baseline_pass_rate: verified.baseline_pass_rate,
        candidate_pass_rate: verified.candidate_pass_rate,
        operator: ack.operator().to_owned(),
        promoted_at: Timestamp::now().as_str().to_owned(),
        weights_changed: false,
        code_changed: false,
    })
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn stemming_collapses_inflections_to_one_term() {
        assert_eq!(stem("invalidation"), "invalidate");
        assert_eq!(stem("invalidating"), "invalidate");
        assert_eq!(stem("invalidated"), "invalidate");
        assert_eq!(stem("invalidate"), "invalidate");
        assert_eq!(stem("deploying"), "deploy");
        assert_eq!(stem("deployed"), "deploy");
        assert_eq!(stem("entries"), "entry");
        assert_eq!(stem("caches"), "cache");
    }

    #[test]
    fn trigram_dice_is_symmetric_and_bounded() {
        let deploy = trigrams("deploy");
        let deployment = trigrams("deployment");
        let forward = dice_coefficient(&deploy, &deployment);
        let backward = dice_coefficient(&deployment, &deploy);
        assert!((forward - backward).abs() < f64::EPSILON);
        assert!(forward > FUZZY_MIN_DICE, "dice was {forward}");
        assert!(forward <= 1.0);
        assert_eq!(dice_coefficient(&deploy, &deploy), 1.0);
    }

    #[test]
    fn missing_trust_label_is_unknown_not_verified() {
        assert_eq!(Verification::from_trust(None), Verification::Unknown);
        assert!(!Verification::from_trust(None).is_verified());
        assert!(!Verification::from_trust(Some("unverified")).is_verified());
        assert!(Verification::from_trust(Some("verified")).is_verified());
        assert!(Verification::from_trust(Some("authoritative")).is_verified());
    }

    #[test]
    fn empty_query_is_refused_by_name() {
        let index = SemanticIndex::build(&[]).expect("empty index");
        let error = index
            .recall(&RecallQuery::new("   ...   "))
            .expect_err("empty query");
        assert_eq!(error.code(), "E_RECALL_QUERY_EMPTY");
    }
}
