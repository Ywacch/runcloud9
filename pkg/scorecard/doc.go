// Package scorecard holds pure comparison math shared by the scorecard, reports, and verdicts.
//
// Noise comparison uses steady-state window means rather than raw 2 s samples, because consecutive
// collector samples are autocorrelated. Callers choose the warm-up cutoff (C-4 uses 60 s by default).
package scorecard
