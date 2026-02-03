# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PM CoE Email Campaigns is a full-stack email marketing application with integrated surveys, contact management, and AI-powered reply analysis.

**Tech Stack:** Node.js/Express backend, vanilla JavaScript frontend, PostgreSQL database, with SendGrid (email), HubSpot (contacts), Gmail OAuth (replies), and Claude API (AI classification) integrations.

## Commands

```bash
npm start          # Start server on http://localhost:3000 (PORT env configurable)
```

No test or lint setup exists. The project has no build step—frontend is served as static files.

## Architecture

### Backend (server.js)

Single-file Express application (~364 lines) containing:

- **Database Layer**: PostgreSQL with auto-initializing schema. Tables: `tags`, `templates`, `campaigns`, `replies`, `gmail_tokens`, `surveys`, `survey_questions`, `survey_responses`, `survey_answers`, `processed_messages`
- **Campaign System**: Draft/schedule/send workflows with SendGrid webhook tracking (delivered, opened, clicked, bounced)
- **Survey System**: Multiple question types, conditional logic, Stripe payment integration, multiple intro styling options
- **Reply Processing**: Gmail OAuth polling (every 5 min), AI classification via Claude for sentiment/interests/suggested actions
- **Contact Management**: HubSpot as source of truth, tag-based filtering with AND/OR logic
- **File Import**: AI-powered extraction from CSV, Excel, PDF, Word, and images

### Frontend (public/index.html)

Single-page application with tabs for Campaigns, Templates, Tags, and Import. Uses Quill.js for rich text editing. No framework or build process.

### Scheduled Processing

- Campaign scheduler: checks every 60 seconds for ready-to-send campaigns
- Gmail checker: polls for new replies every 5 minutes

## Environment Variables

```bash
# Required
DATABASE_URL          # PostgreSQL connection string
SENDGRID_API_KEY      # Email delivery
HUBSPOT_API_KEY       # Contact management
ANTHROPIC_API_KEY     # AI classification (uses claude-sonnet-4-20250514)

# Gmail Integration
GOOGLE_CLIENT_ID
GOOGLE_CLIENT_SECRET
GOOGLE_REDIRECT_URI

# Optional
PORT                  # Default: 3000
APP_URL               # Base URL for survey links
FROM_EMAIL            # Sender address (default: noreply@example.com)
```

Services gracefully degrade if not configured.

## Key Implementation Details

- **Tracking**: Each campaign gets a unique tracking ID embedded in emails for linking replies and webhook events
- **Survey Links**: Template variable `[Survey Link]` is dynamically replaced per-recipient
- **Reply Linking**: Automatic via tracking ID in email headers/body, with manual linking fallback
- **Database**: Auto-creates tables on startup; no migration framework (uses inline ALTER TABLE)
- **File Uploads**: Stored temporarily in `./uploads/` directory
