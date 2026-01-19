# Product Requirements Document: Lumen

**Version:** 1.3
**Last Updated:** January 18, 2026
**Product Type:** Social Media Platform
**Platform:** Web Application (Mobile-First)

---

## 1. Executive Summary
**Lumen** is a mobile-first social platform that merges an audio-first "For You" feed with a magazine-style "News" feed. The key innovation is **dual-mode discovery**: users switch between immersive audio entertainment and premium journalism in a single app, both consumed via TikTok-style vertical snap scrolling.

### Core Value Proposition
*   **For Consumers:** Discover podcasts and news without "decision fatigue." Scroll to listen, scroll to read.
*   **For Creators:** Viral discovery for audio content (podcasts, show clips) usually buried in traditional podcast apps.
*   **For Publishers:** A premium, magazine-quality presentation layer for journalism.

---

## 2. Core Concept & Feeds

### 2.1 The "For You" Feed (Audio-First)
A vertical feed of **Audio-Focused Videos**.
*   **Content:** Podcast clips, interview segments, audio newsletters.
*   **Format:** Full-screen MP4 video. If the source is audio-only, the system generates a video container (visualizer/cover art) so the UX remains consistent.
*   **Experience:** Snap scrolling, auto-play, infinite discovery.

### 2.2 The "News" Feed (Editorial)
A vertical feed of **Curated News Slides**.
*   **Slide Structure:** Each scroll snaps to a single slide containing exactly:
    *   **1 Featured Item:** (e.g., An in-depth article, blog post, or newsletter).
    *   **3 Related Briefs:** (e.g., Tweets, comments, short news updates) related to the feature.
*   **Experience:** Magazine-like reading. The "Related" items provide context or reaction to the main story.

---

## 3. Feature Specifications (MVP)

### 3.1 Interaction
*   **Vertical Snap Scroll:** CSS `scroll-snap` for satisfying, full-page transitions.
*   **Auto-Play:** Videos play immediately upon landing.
*   **Actions:** Like (Double-tap), Comment, Share, Bookmark.

### 3.2 Content Types (MVP)
*   **News Featured:** Articles, Blogs.
*   **News Related:** Tweets, Comments, Short Articles.
*   **For You:** MP4 Videos (Source: Podcasts, Uploads).

---

## 4. Discovery Strategy
*   **Ranking:** AI-driven vector similarity (semantic match) + Freshness + Global Trends.
*   **Diversity:** Logic to prevent seeing the same creator/topic 3 times in a row.

---

## 5. Success Metrics
*   **Session Duration:** Target > 15 mins.
*   **Audio Completion Rate:** Target > 40%.
*   **Feed Switch Rate:** % of users using *both* feeds in one session.
