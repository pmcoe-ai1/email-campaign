// v39 - Lead capture & nurture engine
require('dotenv').config();
const express = require('express');
const sgMail = require('@sendgrid/mail');
const hubspot = require('@hubspot/api-client');
const { google } = require('googleapis');
const Anthropic = require('@anthropic-ai/sdk');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const XLSX = require('xlsx');
const pdfParse = require('pdf-parse');
const mammoth = require('mammoth');
const { Pool } = require('pg');

const app = express();
app.use(express.json({ limit: '50mb' }));
app.use(express.static('public'));

app.get('/health', (req, res) => res.status(200).send('OK'));
app.get('/_health', (req, res) => res.status(200).send('OK'));

const upload = multer({ dest: 'uploads/', limits: { fileSize: 10 * 1024 * 1024 } });

let pool = null;
async function initDatabase() {
  if (!process.env.DATABASE_URL) return;
  pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
  
  await pool.query(`CREATE TABLE IF NOT EXISTS tags (id SERIAL PRIMARY KEY, name VARCHAR(255) UNIQUE NOT NULL, created_at TIMESTAMP DEFAULT NOW())`);
  await pool.query(`CREATE TABLE IF NOT EXISTS templates (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, subject VARCHAR(500), body TEXT, created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())`);
  await pool.query(`CREATE TABLE IF NOT EXISTS campaigns (id VARCHAR(50) PRIMARY KEY, name VARCHAR(255), subject VARCHAR(500), body TEXT, status VARCHAR(50) DEFAULT 'draft', campaign_type VARCHAR(20) DEFAULT 'email', survey_id VARCHAR(50), contact_ids TEXT, tracking_id VARCHAR(20), scheduled_time TIMESTAMP, sent_at TIMESTAMP, recipients JSONB, created_at TIMESTAMP DEFAULT NOW())`);
  try { await pool.query("ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS campaign_type VARCHAR(20) DEFAULT 'email'"); } catch(e) {}
  try { await pool.query("ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS survey_id VARCHAR(50)"); } catch(e) {}
  await pool.query(`CREATE TABLE IF NOT EXISTS replies (id VARCHAR(100) PRIMARY KEY, campaign_id VARCHAR(50), from_email VARCHAR(255), from_name VARCHAR(255), subject VARCHAR(500), body TEXT, classification JSONB, status VARCHAR(50), contact_id VARCHAR(50), contact_name VARCHAR(255), received_at TIMESTAMP, follow_up_sent BOOLEAN DEFAULT FALSE)`);
  await pool.query(`CREATE TABLE IF NOT EXISTS gmail_tokens (id INTEGER PRIMARY KEY DEFAULT 1, tokens JSONB, updated_at TIMESTAMP DEFAULT NOW())`);
  await pool.query(`CREATE TABLE IF NOT EXISTS processed_messages (message_id VARCHAR(100) PRIMARY KEY, processed_at TIMESTAMP DEFAULT NOW())`);
  await pool.query(`CREATE TABLE IF NOT EXISTS surveys (id VARCHAR(50) PRIMARY KEY, name VARCHAR(255) NOT NULL, description TEXT, cta_text VARCHAR(255), cta_action VARCHAR(50) DEFAULT 'url', cta_url VARCHAR(500), created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())`);
  try { await pool.query("ALTER TABLE surveys ADD COLUMN IF NOT EXISTS cta_text VARCHAR(255)"); } catch(e) {}
  try { await pool.query("ALTER TABLE surveys ADD COLUMN IF NOT EXISTS cta_action VARCHAR(50) DEFAULT 'url'"); } catch(e) {}
  try { await pool.query("ALTER TABLE surveys ADD COLUMN IF NOT EXISTS cta_url VARCHAR(500)"); } catch(e) {}
  await pool.query(`CREATE TABLE IF NOT EXISTS survey_questions (id SERIAL PRIMARY KEY, survey_id VARCHAR(50) NOT NULL, question_text TEXT NOT NULL, question_type VARCHAR(50) NOT NULL, options JSONB, show_stats BOOLEAN DEFAULT TRUE, required BOOLEAN DEFAULT FALSE, order_num INTEGER DEFAULT 0, conditional_logic JSONB, stripe_link VARCHAR(500), intro_content TEXT, intro_style VARCHAR(50) DEFAULT 'modern')`);
  try { await pool.query("ALTER TABLE survey_questions ADD COLUMN IF NOT EXISTS intro_content TEXT"); } catch(e) {}
  try { await pool.query("ALTER TABLE survey_questions ADD COLUMN IF NOT EXISTS intro_style VARCHAR(50) DEFAULT 'modern'"); } catch(e) {}
  await pool.query(`CREATE TABLE IF NOT EXISTS survey_responses (id VARCHAR(50) PRIMARY KEY, survey_id VARCHAR(50) NOT NULL, campaign_id VARCHAR(50), contact_id VARCHAR(50), email VARCHAR(255), first_name VARCHAR(255), last_name VARCHAR(255), submitted_at TIMESTAMP DEFAULT NOW())`);
  await pool.query(`CREATE TABLE IF NOT EXISTS survey_answers (id SERIAL PRIMARY KEY, response_id VARCHAR(50) NOT NULL, question_id INTEGER NOT NULL, answer_value TEXT)`);
  
  // Lead capture tables
  await pool.query(`CREATE TABLE IF NOT EXISTS leads (
    id VARCHAR(50) PRIMARY KEY, email VARCHAR(255) NOT NULL, first_name VARCHAR(255), last_name VARCHAR(255),
    course VARCHAR(50), region VARCHAR(100), promo_code VARCHAR(50), enrollment_url TEXT,
    score INTEGER DEFAULT 0, status VARCHAR(30) DEFAULT 'new', gmail_message_id VARCHAR(100),
    hubspot_contact_id VARCHAR(50), captured_at TIMESTAMP DEFAULT NOW(), converted_at TIMESTAMP,
    UNIQUE(email, course)
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS lead_sequences (
    id SERIAL PRIMARY KEY, lead_id VARCHAR(50) NOT NULL REFERENCES leads(id),
    step INTEGER NOT NULL, subject VARCHAR(500), body TEXT,
    scheduled_for TIMESTAMP NOT NULL, sent_at TIMESTAMP, status VARCHAR(30) DEFAULT 'pending',
    sendgrid_message_id VARCHAR(100), opened BOOLEAN DEFAULT FALSE, clicked BOOLEAN DEFAULT FALSE
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS lead_sequence_configs (
    id SERIAL PRIMARY KEY, course VARCHAR(50) NOT NULL, step INTEGER NOT NULL,
    delay_days INTEGER NOT NULL, subject VARCHAR(500) NOT NULL, body TEXT NOT NULL,
    enabled BOOLEAN DEFAULT TRUE, UNIQUE(course, step)
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS lead_processed_emails (
    message_id VARCHAR(100) PRIMARY KEY, processed_at TIMESTAMP DEFAULT NOW()
  )`);
  
  // Seed default nurture sequences if empty
  const seqCount = await pool.query('SELECT COUNT(*) FROM lead_sequence_configs');
  if (parseInt(seqCount.rows[0].count) === 0) {
    const courses = ['PMP', 'CAPM', 'PMI-CP'];
    for (const course of courses) {
      const sequences = [
        { step: 1, delay: 2, subject: `Your ${course} Certification Journey Starts Here`, body: `<p>Hi [First Name],</p><p>Thank you for your interest in ${course} certification! We wanted to share what our recent graduates are saying:</p><p><em>"PM CoE's ${course} training was exactly what I needed. The instructors were knowledgeable and the materials were comprehensive."</em></p><p>Ready to take the next step? Your discount code is still active.</p><p><a href="[Enrollment URL]" style="display:inline-block;padding:12px 24px;background:#e94560;color:white;text-decoration:none;border-radius:6px;font-weight:500">Enroll Now with Your Discount</a></p><p>PM CoE Team</p>` },
        { step: 2, delay: 5, subject: `Your ${course} discount expires soon!`, body: `<p>Hi [First Name],</p><p>Just a quick reminder — your ${course} promotion code <strong>[Promo Code]</strong> is valid for 14 days from when you received it.</p><p>Don't miss out on this discounted rate for our comprehensive ${course} certification training.</p><p><a href="[Enrollment URL]" style="display:inline-block;padding:12px 24px;background:#e94560;color:white;text-decoration:none;border-radius:6px;font-weight:500">Use Your Discount Now</a></p><p>PM CoE Team</p>` },
        { step: 3, delay: 9, subject: `Everything you need to know about ${course} certification`, body: `<p>Hi [First Name],</p><p>Did you know?</p><ul><li>${course === 'PMP' ? 'PMP-certified professionals earn 25% more on average than non-certified peers' : course === 'CAPM' ? 'CAPM certification is the perfect first step into project management' : 'PMI-CP certification demonstrates your expertise in construction project management'}</li><li>Our courses are ${course === 'PMP' || course === 'CAPM' ? 'G.I. Bill approved' : 'PMI Authorized Training Partner certified'}</li><li>We offer live instructor-led training across the US, Canada, and Australia</li></ul><p><a href="[Enrollment URL]" style="display:inline-block;padding:12px 24px;background:#e94560;color:white;text-decoration:none;border-radius:6px;font-weight:500">Learn More & Enroll</a></p><p>PM CoE Team</p>` },
        { step: 4, delay: 14, subject: `Last chance: Your ${course} discount is expiring`, body: `<p>Hi [First Name],</p><p>This is a final reminder that your ${course} promotion code <strong>[Promo Code]</strong> is about to expire.</p><p>If you have any questions about the course, schedule, or pricing, just reply to this email — we're here to help.</p><p><a href="[Enrollment URL]" style="display:inline-block;padding:12px 24px;background:#e94560;color:white;text-decoration:none;border-radius:6px;font-weight:500">Enroll Before It Expires</a></p><p>PM CoE Team</p>` }
      ];
      for (const seq of sequences) {
        await pool.query('INSERT INTO lead_sequence_configs (course, step, delay_days, subject, body) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING', [course, seq.step, seq.delay, seq.subject, seq.body]);
      }
    }
  }
  
  // Load processed lead email IDs into memory
  const leadMsgResult = await pool.query('SELECT message_id FROM lead_processed_emails');
  leadMsgResult.rows.forEach(r => leadProcessedIds.add(r.message_id));
  
  const tplCount = await pool.query('SELECT COUNT(*) FROM templates');
  if (parseInt(tplCount.rows[0].count) === 0) {
    const defaultTemplates = [
      { name: 'Welcome - PM CoE Network', subject: 'Introducing PM CoE Network', body: '<p>Hi [First Name],</p><p>We are launching <strong>PM CoE Network</strong>—a program for our alumni.</p><p>PM CoE Team</p>' },
      { name: 'PDU App', subject: "Earn PDUs with PM CoE's PDU App", body: '<p>Hi [First Name],</p><p>Our PDU App offers free and premium content fully integrated with PMI.</p><p>PM CoE Team</p>' },
      { name: 'Referral Program', subject: 'PM CoE Referral Program', body: '<p>Hi [First Name],</p><p>Earn cash rewards by referring colleagues.</p><p>PM CoE Team</p>' },
      { name: 'Career Coaching', subject: 'Career Coaching at PM CoE', body: '<p>Hi [First Name],</p><p>Our coaching program helps you advance your career.</p><p>PM CoE Team</p>' },
      { name: 'Job Connections', subject: 'Access PM CoE Job Connections', body: '<p>Hi [First Name],</p><p>Access curated job opportunities for certified project managers.</p><p>PM CoE Team</p>' },
      { name: 'Next Certification', subject: 'Your Next Certification Path', body: '<p>Hi [First Name],</p><p>Get preferred pricing on PMI-ACP, PMI-CP, and more.</p><p>PM CoE Team</p>' }
    ];
    for (const t of defaultTemplates) await pool.query('INSERT INTO templates (name, subject, body) VALUES ($1, $2, $3)', [t.name, t.subject, t.body]);
  }
  
  const surveyCount = await pool.query('SELECT COUNT(*) FROM surveys');
  if (parseInt(surveyCount.rows[0].count) === 0) {
    await pool.query("INSERT INTO surveys (id, name, description) VALUES ('pmp-checkin', 'PMP Journey Check-in', 'Follow-up survey for PMP course alumni')");
    await pool.query("INSERT INTO survey_questions (survey_id, question_text, question_type, options, order_num) VALUES ('pmp-checkin', 'Have you taken the PMP exam yet?', 'single_choice', '[\"Yes, I passed 🎉\",\"Yes, but I didn''t pass\",\"No, I haven''t taken it yet\"]', 1)");
    await pool.query("INSERT INTO survey_questions (survey_id, question_text, question_type, options, order_num, conditional_logic, stripe_link) VALUES ('pmp-checkin', 'Would you like to join the PM CoE Network? ($5/month)', 'single_choice', '[\"Yes, I want to join\",\"Maybe later\",\"No thanks\"]', 2, '{\"question\":1,\"value\":\"Yes, I passed 🎉\"}', 'https://buy.stripe.com/membership')");
    await pool.query("INSERT INTO survey_questions (survey_id, question_text, question_type, options, order_num, conditional_logic) VALUES ('pmp-checkin', 'What would help you most for your next attempt?', 'single_choice', '[\"More practice questions\",\"Content review / refresher\",\"1:1 coaching\",\"Gap analysis\"]', 3, '{\"question\":1,\"value\":\"Yes, but I didn''t pass\"}')");
    await pool.query("INSERT INTO survey_questions (survey_id, question_text, question_type, options, order_num, conditional_logic, stripe_link) VALUES ('pmp-checkin', 'Would you like study support for $55/month?', 'single_choice', '[\"Yes, I''m interested\",\"Maybe later\",\"No thanks\"]', 4, '{\"question\":1,\"values\":[\"Yes, but I didn''t pass\",\"No, I haven''t taken it yet\"]}', 'https://buy.stripe.com/study')");
    await pool.query("INSERT INTO survey_questions (survey_id, question_text, question_type, options, order_num, required, show_stats) VALUES ('pmp-checkin', 'Anything else you''d like to share?', 'long_text', '[]', 5, false, false)");
  }
  
  const tokenResult = await pool.query('SELECT tokens FROM gmail_tokens WHERE id = 1');
  if (tokenResult.rows.length && tokenResult.rows[0].tokens) { 
    gmailTokens = tokenResult.rows[0].tokens; 
    console.log('Loaded Gmail tokens from DB');
    console.log('Has refresh_token:', !!gmailTokens.refresh_token);
    if (oauth2Client) oauth2Client.setCredentials(gmailTokens); 
  } else {
    console.log('No Gmail tokens found in DB');
  }
  const msgResult = await pool.query('SELECT message_id FROM processed_messages');
  msgResult.rows.forEach(r => processedMessageIds.add(r.message_id));
  console.log('Database initialized');
}

let sgInitialized = false, hubspotClient = null, anthropic = null, oauth2Client = null;
try { if (process.env.SENDGRID_API_KEY) { sgMail.setApiKey(process.env.SENDGRID_API_KEY); sgInitialized = true; } } catch (e) {}
try { if (process.env.HUBSPOT_API_KEY) { hubspotClient = new hubspot.Client({ accessToken: process.env.HUBSPOT_API_KEY }); } } catch (e) {}
try { if (process.env.ANTHROPIC_API_KEY) { anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY }); } } catch (e) {}
try { if (process.env.GOOGLE_CLIENT_ID) { oauth2Client = new google.auth.OAuth2(process.env.GOOGLE_CLIENT_ID, process.env.GOOGLE_CLIENT_SECRET, process.env.GOOGLE_REDIRECT_URI); } } catch (e) {}

let gmailTokens = null;
const processedMessageIds = new Set();
const leadProcessedIds = new Set();
function generateTrackingId() { return crypto.randomBytes(4).toString('hex').toUpperCase(); }
function generateId() { return crypto.randomBytes(8).toString('hex'); }
function getAppUrl() { return process.env.APP_URL || (process.env.RAILWAY_PUBLIC_DOMAIN ? 'https://' + process.env.RAILWAY_PUBLIC_DOMAIN : 'http://localhost:' + (process.env.PORT || 3000)); }
const DEFAULT_IMPORT_PROMPT = `Extract contacts as JSON array: [{"firstName":"","lastName":"","email":"","company":"","valid":true}]. If no email, set valid:false. Return ONLY JSON.`;

// Auth
app.get('/api/auth/google', (req, res) => { if (!oauth2Client) return res.status(500).json({ error: 'Not configured' }); res.redirect(oauth2Client.generateAuthUrl({ access_type: 'offline', prompt: 'consent', scope: ['https://www.googleapis.com/auth/gmail.readonly', 'https://www.googleapis.com/auth/gmail.send'] })); });
app.get('/api/auth/google/callback', async (req, res) => { 
  try { 
    const { tokens } = await oauth2Client.getToken(req.query.code); 
    console.log('OAuth callback - tokens received (refresh_token:', !!tokens.refresh_token, ')');
    gmailTokens = tokens; 
    oauth2Client.setCredentials(tokens); 
    if (pool) await pool.query('INSERT INTO gmail_tokens (id, tokens, updated_at) VALUES (1, $1, NOW()) ON CONFLICT (id) DO UPDATE SET tokens = $1, updated_at = NOW()', [JSON.stringify(tokens)]); 
    res.redirect('/?gmail=connected'); 
  } catch (e) { 
    console.error('OAuth callback error:', e.message);
    res.redirect('/?gmail=error'); 
  } 
});
app.get('/api/auth/gmail/status', (req, res) => res.json({ connected: !!gmailTokens, hasRefreshToken: !!gmailTokens?.refresh_token }));
app.post('/api/auth/gmail/disconnect', async (req, res) => {
  gmailTokens = null;
  if (pool) await pool.query('DELETE FROM gmail_tokens WHERE id = 1');
  res.json({ success: true, message: 'Gmail disconnected. Please reconnect.' });
});

// Contacts
app.get('/api/contacts', async (req, res) => { if (!hubspotClient) return res.status(500).json({ error: 'HubSpot not configured' }); try { const r = await hubspotClient.crm.contacts.basicApi.getPage(100, undefined, ['firstname', 'lastname', 'email', 'customer_tag']); res.json(r.results); } catch (e) { res.status(500).json({ error: e.message }); } });

// Tags
app.get('/api/tags', async (req, res) => { try { const tagCounts = {}; if (hubspotClient) { try { const r = await hubspotClient.crm.contacts.basicApi.getPage(100, undefined, ['customer_tag']); r.results.forEach(c => { (c.properties.customer_tag || '').split(',').map(t => t.trim()).filter(t => t).forEach(t => { tagCounts[t] = (tagCounts[t] || 0) + 1; }); }); } catch (e) {} } if (pool) { const dbResult = await pool.query('SELECT name FROM tags'); dbResult.rows.forEach(row => { if (!tagCounts[row.name]) tagCounts[row.name] = 0; }); } res.json(Object.entries(tagCounts).map(([name, count]) => ({ name, count })).sort((a, b) => a.name.localeCompare(b.name))); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/api/tags', async (req, res) => { const { name } = req.body; if (!name?.trim()) return res.status(400).json({ error: 'Tag name required' }); if (pool) { try { await pool.query('INSERT INTO tags (name) VALUES ($1) ON CONFLICT (name) DO NOTHING', [name.trim()]); } catch (e) {} } res.json({ success: true, tag: name.trim() }); });
app.put('/api/tags/:oldName', async (req, res) => { const { oldName } = req.params; const { newName } = req.body; if (!newName?.trim()) return res.status(400).json({ error: 'New name required' }); if (pool) { try { await pool.query('UPDATE tags SET name = $1 WHERE name = $2', [newName.trim(), oldName]); } catch (e) {} } if (hubspotClient) { try { const r = await hubspotClient.crm.contacts.basicApi.getPage(100, undefined, ['customer_tag']); for (const c of r.results) { const tags = (c.properties.customer_tag || '').split(',').map(t => t.trim()).filter(t => t); if (tags.includes(oldName)) { await hubspotClient.crm.contacts.basicApi.update(c.id, { properties: { customer_tag: tags.map(t => t === oldName ? newName.trim() : t).join(',') } }); } } } catch (e) {} } res.json({ success: true }); });
app.delete('/api/tags/:name', async (req, res) => { const { name } = req.params; if (pool) { try { await pool.query('DELETE FROM tags WHERE name = $1', [name]); } catch (e) {} } if (hubspotClient) { try { const r = await hubspotClient.crm.contacts.basicApi.getPage(100, undefined, ['customer_tag']); for (const c of r.results) { const tags = (c.properties.customer_tag || '').split(',').map(t => t.trim()).filter(t => t); if (tags.includes(name)) { await hubspotClient.crm.contacts.basicApi.update(c.id, { properties: { customer_tag: tags.filter(t => t !== name).join(',') } }); } } } catch (e) {} } res.json({ success: true }); });
app.post('/api/contacts/tags', async (req, res) => { if (!hubspotClient) return res.status(500).json({ error: 'HubSpot not configured' }); try { const { contactIds, action, tag } = req.body; if (action === 'add' && pool) { try { await pool.query('INSERT INTO tags (name) VALUES ($1) ON CONFLICT (name) DO NOTHING', [tag]); } catch (e) {} } for (const id of contactIds) { const c = await hubspotClient.crm.contacts.basicApi.getById(id, ['customer_tag']); let tags = (c.properties.customer_tag || '').split(',').map(t => t.trim()).filter(t => t); if (action === 'add' && !tags.includes(tag)) tags.push(tag); else if (action === 'remove') tags = tags.filter(t => t !== tag); await hubspotClient.crm.contacts.basicApi.update(id, { properties: { customer_tag: tags.join(',') } }); } res.json({ success: true }); } catch (e) { res.status(500).json({ error: e.message }); } });

// Templates
app.get('/api/templates', async (req, res) => { if (!pool) return res.json([]); try { const result = await pool.query('SELECT * FROM templates ORDER BY name'); res.json(result.rows); } catch (e) { res.status(500).json({ error: e.message }); } });
app.get('/api/templates/:id', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); try { const result = await pool.query('SELECT * FROM templates WHERE id = $1', [req.params.id]); if (!result.rows.length) return res.status(404).json({ error: 'Not found' }); res.json(result.rows[0]); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/api/templates', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); const { name, subject, body } = req.body; if (!name?.trim()) return res.status(400).json({ error: 'Name required' }); try { const result = await pool.query('INSERT INTO templates (name, subject, body) VALUES ($1, $2, $3) RETURNING *', [name.trim(), subject || '', body || '']); res.json(result.rows[0]); } catch (e) { res.status(500).json({ error: e.message }); } });
app.put('/api/templates/:id', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); const { name, subject, body } = req.body; try { const result = await pool.query('UPDATE templates SET name = $1, subject = $2, body = $3, updated_at = NOW() WHERE id = $4 RETURNING *', [name, subject, body, req.params.id]); if (!result.rows.length) return res.status(404).json({ error: 'Not found' }); res.json(result.rows[0]); } catch (e) { res.status(500).json({ error: e.message }); } });
app.delete('/api/templates/:id', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); try { await pool.query('DELETE FROM templates WHERE id = $1', [req.params.id]); res.json({ success: true }); } catch (e) { res.status(500).json({ error: e.message }); } });

// Surveys
app.get('/api/surveys', async (req, res) => { if (!pool) return res.json([]); try { const result = await pool.query('SELECT s.*, COUNT(sq.id) as question_count FROM surveys s LEFT JOIN survey_questions sq ON s.id = sq.survey_id GROUP BY s.id ORDER BY s.name'); res.json(result.rows); } catch (e) { res.status(500).json({ error: e.message }); } });
app.get('/api/surveys/:id', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); try { const survey = await pool.query('SELECT * FROM surveys WHERE id = $1', [req.params.id]); if (!survey.rows.length) return res.status(404).json({ error: 'Not found' }); const questions = await pool.query('SELECT * FROM survey_questions WHERE survey_id = $1 ORDER BY order_num', [req.params.id]); res.json({ ...survey.rows[0], questions: questions.rows }); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/api/surveys', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); const { name, description, cta_text, cta_action, cta_url } = req.body; if (!name?.trim()) return res.status(400).json({ error: 'Name required' }); const id = generateId(); try { const result = await pool.query('INSERT INTO surveys (id, name, description, cta_text, cta_action, cta_url) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *', [id, name.trim(), description || '', cta_text || 'Try our Exam Readiness Assessment', cta_action || 'url', cta_url || 'https://path2pmp.com/exam-test']); res.json({ ...result.rows[0], questions: [] }); } catch (e) { res.status(500).json({ error: e.message }); } });
app.put('/api/surveys/:id', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); const { name, description, cta_text, cta_action, cta_url } = req.body; try { const result = await pool.query('UPDATE surveys SET name = $1, description = $2, cta_text = $3, cta_action = $4, cta_url = $5, updated_at = NOW() WHERE id = $6 RETURNING *', [name, description, cta_text, cta_action, cta_url, req.params.id]); if (!result.rows.length) return res.status(404).json({ error: 'Not found' }); res.json(result.rows[0]); } catch (e) { res.status(500).json({ error: e.message }); } });
app.delete('/api/surveys/:id', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); try { await pool.query('DELETE FROM survey_questions WHERE survey_id = $1', [req.params.id]); await pool.query('DELETE FROM surveys WHERE id = $1', [req.params.id]); res.json({ success: true }); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/api/surveys/:id/questions', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); const { question_text, question_type, options, show_stats, required, order_num, conditional_logic, stripe_link, intro_content, intro_style } = req.body; try { const result = await pool.query('INSERT INTO survey_questions (survey_id, question_text, question_type, options, show_stats, required, order_num, conditional_logic, stripe_link, intro_content, intro_style) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING *', [req.params.id, question_text, question_type, JSON.stringify(options || []), show_stats !== false, required || false, order_num || 0, conditional_logic ? JSON.stringify(conditional_logic) : null, stripe_link || null, intro_content || null, intro_style || 'modern']); res.json(result.rows[0]); } catch (e) { res.status(500).json({ error: e.message }); } });
app.put('/api/surveys/:id/questions/:qid', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); const { question_text, question_type, options, show_stats, required, order_num, conditional_logic, stripe_link, intro_content, intro_style } = req.body; try { const result = await pool.query('UPDATE survey_questions SET question_text = $1, question_type = $2, options = $3, show_stats = $4, required = $5, order_num = $6, conditional_logic = $7, stripe_link = $8, intro_content = $9, intro_style = $10 WHERE id = $11 AND survey_id = $12 RETURNING *', [question_text, question_type, JSON.stringify(options || []), show_stats, required, order_num, conditional_logic ? JSON.stringify(conditional_logic) : null, stripe_link, intro_content || null, intro_style || 'modern', req.params.qid, req.params.id]); if (!result.rows.length) return res.status(404).json({ error: 'Not found' }); res.json(result.rows[0]); } catch (e) { res.status(500).json({ error: e.message }); } });
app.delete('/api/surveys/:id/questions/:qid', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); try { await pool.query('DELETE FROM survey_questions WHERE id = $1 AND survey_id = $2', [req.params.qid, req.params.id]); res.json({ success: true }); } catch (e) { res.status(500).json({ error: e.message }); } });

// Public Survey Form
app.get('/survey/:id', async (req, res) => {
  if (!pool) return res.status(500).send('Database not configured');
  try {
    const survey = await pool.query('SELECT * FROM surveys WHERE id = $1', [req.params.id]);
    if (!survey.rows.length) return res.status(404).send('Survey not found');
    const questions = await pool.query('SELECT * FROM survey_questions WHERE survey_id = $1 ORDER BY order_num', [req.params.id]);
    res.send(generateSurveyHTML(survey.rows[0], questions.rows, req.query));
  } catch (e) { res.status(500).send('Error loading survey'); }
});

function generateSurveyHTML(survey, questions, params) {
  function esc(s) { return String(s||'').replace(/\\/g,'\\\\').replace(/'/g,"\\'").replace(/</g,'\\x3C').replace(/>/g,'\\x3E').replace(/"/g,'\\x22'); }
  const email = esc(params.email), fname = esc(params.fname), lname = esc(params.lname), campaignId = esc(params.campaign_id);
  const questionsJSON = JSON.stringify(questions.map(q => ({ id: q.id, text: q.question_text, type: q.question_type, options: q.options || [], required: q.required, showStats: q.show_stats, conditional: q.conditional_logic, stripeLink: q.stripe_link, introContent: q.intro_content, introStyle: q.intro_style || 'modern' })));
  return `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>${survey.name} | PM CoE</title><link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;600;700&display=swap" rel="stylesheet"><style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Poppins',sans-serif;background:linear-gradient(135deg,#1a1a2e 0%,#16213e 50%,#0f3460 100%);min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:40px 20px;color:#333}.logo{margin-bottom:30px}.logo img{height:60px}.container{background:#fff;border-radius:16px;box-shadow:0 25px 50px -12px rgba(0,0,0,0.4);max-width:600px;width:100%;overflow:hidden}.header{background:linear-gradient(135deg,#e94560 0%,#c73e54 100%);padding:30px 40px;color:white}.header h1{font-size:1.6rem;font-weight:600;margin-bottom:8px}.header p{font-size:0.95rem;opacity:0.9}.body{padding:35px 40px}.progress{height:4px;background:#e9ecef;border-radius:2px;margin-bottom:25px;overflow:hidden}.progress-fill{height:100%;background:linear-gradient(135deg,#e94560 0%,#c73e54 100%);transition:width 0.4s ease}.question{display:none;animation:fadeIn 0.4s ease}.question.active{display:block}@keyframes fadeIn{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}.question-label{font-size:1.05rem;font-weight:500;color:#1a1a2e;margin-bottom:18px;display:block}.options{display:flex;flex-direction:column;gap:12px}.option{position:relative}.option input{position:absolute;opacity:0}.option label{display:block;padding:16px 20px;background:#f8f9fa;border:2px solid #e9ecef;border-radius:10px;cursor:pointer;transition:all 0.2s;font-size:0.95rem;color:#495057}.option label:hover{border-color:#e94560;background:#fff5f7}.option input:checked+label{border-color:#e94560;background:#fff5f7;color:#c73e54;font-weight:500}textarea{width:100%;padding:14px 18px;border:2px solid #e9ecef;border-radius:10px;font-family:inherit;font-size:0.95rem;resize:vertical;min-height:100px}textarea:focus{outline:none;border-color:#e94560}.btn{display:inline-block;padding:14px 32px;background:linear-gradient(135deg,#e94560 0%,#c73e54 100%);color:white;border:none;border-radius:8px;font-family:inherit;font-size:1rem;font-weight:500;cursor:pointer;transition:all 0.2s;margin-top:20px}.btn:hover{transform:translateY(-2px);box-shadow:0 8px 20px rgba(233,69,96,0.3)}.btn-secondary{background:#6c757d;margin-right:10px}.btn-stripe{background:linear-gradient(135deg,#28a745 0%,#20c997 100%);padding:16px 40px;font-size:1.1rem}.btn-group{display:flex;gap:10px;margin-top:25px;flex-wrap:wrap}.success{text-align:center;padding:20px 0;display:none}.success.active{display:block;animation:fadeIn 0.4s ease}.success-icon{width:80px;height:80px;background:linear-gradient(135deg,#28a745 0%,#20c997 100%);border-radius:50%;display:flex;align-items:center;justify-content:center;margin:0 auto 20px}.success-icon svg{width:40px;height:40px;stroke:white;stroke-width:3}.success h2{color:#1a1a2e;font-size:1.4rem;margin-bottom:10px}.success p{color:#6c757d}.cta-link{display:inline-block;margin-top:20px;padding:12px 24px;background:#f8f9fa;border:2px solid #e94560;color:#e94560;border-radius:8px;text-decoration:none;font-weight:500}.cta-link:hover{background:#e94560;color:white}.stripe-box{text-align:center;padding:25px;background:#f8f9fa;border-radius:12px;margin-bottom:20px}.stripe-box h3{font-size:1.2rem;color:#1a1a2e;margin-bottom:12px}.stripe-box p{font-size:0.95rem;color:#495057;margin-bottom:20px}@media(max-width:600px){body{padding:20px 15px}.header,.body{padding:25px 20px}.btn{width:100%;text-align:center}.btn-group{flex-direction:column}.btn-secondary{margin-right:0;margin-bottom:10px}}
.intro-modern{background:#f8f9fa;border-radius:0 12px 12px 0;padding:24px;margin-bottom:24px;border-left:4px solid #e94560}.intro-modern-header{text-align:center}.intro-modern h2{color:#1a1a2e;font-size:1.4rem;margin-bottom:8px}.intro-modern .subhead{color:#6c757d;margin-bottom:16px}.intro-modern .price{font-size:2.5rem;font-weight:700;color:#e94560;margin:16px 0 4px}.intro-modern .price-caption{color:#6c757d;font-size:0.9rem;margin-bottom:20px}.intro-modern .benefit{padding:12px 0;border-top:1px solid #e9ecef;text-align:left !important}.intro-modern .benefit:first-of-type{border-top:none}.intro-modern .benefit-title{font-weight:600;color:#1a1a2e;display:inline;text-align:left}.intro-modern .benefit-badge{font-size:0.7rem;padding:2px 8px;border-radius:10px;margin-left:8px;font-weight:600;vertical-align:middle}.intro-modern .benefit-badge.free{background:#d4edda;color:#155724}.intro-modern .benefit-badge.earn{background:#fff3cd;color:#856404}.intro-modern .benefit-badge.discount{background:#cce5ff;color:#004085}.intro-modern .benefit-desc{color:#6c757d;font-size:0.85rem;margin-top:4px;display:block;padding-left:0;margin-left:0;text-align:left !important}
.intro-gradient{background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);border-radius:12px;padding:28px;margin-bottom:24px;color:white;text-align:center}.intro-gradient h2{font-size:1.5rem;margin-bottom:8px}.intro-gradient .subhead{opacity:0.9;margin-bottom:16px}.intro-gradient .price{font-size:3rem;font-weight:700;margin:16px 0 4px}.intro-gradient .price-caption{opacity:0.8;font-size:0.9rem;margin-bottom:20px}.intro-gradient .benefits-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;text-align:left}.intro-gradient .benefit{background:rgba(255,255,255,0.15);border-radius:8px;padding:12px}.intro-gradient .benefit-title{font-weight:600;font-size:0.9rem}.intro-gradient .benefit-badge{font-size:0.65rem;padding:2px 6px;border-radius:8px;margin-left:6px;background:rgba(255,255,255,0.3)}.intro-gradient .benefit-desc{opacity:0.85;font-size:0.8rem;margin-top:4px}
.intro-minimal{border:1px solid #e9ecef;border-radius:8px;padding:20px;margin-bottom:24px;text-align:center}.intro-minimal h2{color:#1a1a2e;font-size:1.2rem;margin-bottom:12px;padding-bottom:12px;border-bottom:1px solid #e9ecef}.intro-minimal .price{font-size:1.8rem;font-weight:600;color:#1a1a2e}.intro-minimal .price-caption{color:#6c757d;font-size:0.85rem;margin-bottom:16px}.intro-minimal .benefit{padding:8px 0;color:#495057;font-size:0.9rem;text-align:left}.intro-minimal .benefit-title{font-weight:500}.intro-minimal .benefit-badge{font-size:0.7rem;color:#e94560;margin-left:6px}
.intro-dark{background:#1a1a2e;border-radius:12px;padding:28px;margin-bottom:24px;color:white;text-align:center}.intro-dark h2{font-size:1.4rem;margin-bottom:8px}.intro-dark .subhead{color:#a0a0a0;margin-bottom:16px}.intro-dark .price{font-size:2.5rem;font-weight:700;color:#e94560;margin:16px 0 4px}.intro-dark .price-caption{color:#a0a0a0;font-size:0.9rem;margin-bottom:20px}.intro-dark .benefit{padding:10px 0;border-top:1px solid #2a2a4e;text-align:left}.intro-dark .benefit:first-of-type{border-top:none}.intro-dark .benefit-title{font-weight:500}.intro-dark .benefit-badge{font-size:0.7rem;padding:2px 8px;border-radius:10px;margin-left:8px;background:#e94560}.intro-dark .benefit-desc{color:#a0a0a0;font-size:0.85rem;margin-top:4px}
</style></head><body><div class="logo"><img src="https://www.pm-coe.com/wp-content/uploads/2024/07/pmcoe_logo_high-res.png" alt="PM CoE"></div><div class="container"><div class="header"><h1>${survey.name}</h1><p>${survey.description || 'Help us understand where you are so we can support you better'}</p></div><div class="body"><div class="progress"><div class="progress-fill" id="progressBar" style="width:0%"></div></div><div id="questions"></div><div class="success" id="success"><div class="success-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><polyline points="20 6 9 17 4 12"></polyline></svg></div><h2>Thank you!</h2><p>We've received your response and will be in touch soon.</p>${survey.cta_action === 'none' ? '' : survey.cta_action === 'close' ? '<button onclick="window.close()" class="cta-link">' + (survey.cta_text || 'Close') + '</button>' : '<a href="' + (survey.cta_url || 'https://path2pmp.com/exam-test') + '" class="cta-link">' + (survey.cta_text || 'Try our Exam Readiness Assessment') + '</a>'}</div></div></div><script>const questions=${questionsJSON};const surveyId='${survey.id}';const campaignId='${campaignId}';const userEmail='${email}';const userFirstName='${fname}';const userLastName='${lname}';let currentQ=0;const answers={};
function parseIntroMarkdown(md,style){if(!md)return'';const lines=md.trim().split('\\n');let html='<div class="intro-'+style+'"><div class="intro-modern-header">';let inBenefits=false;for(let line of lines){line=line.trim();if(!line)continue;if(line.startsWith('# ')){html+='<h2>'+line.slice(2)+'</h2>';}else if(line.startsWith('## ')){const priceMatch=line.slice(3).match(/^\\$([0-9.]+)(.*)$/);if(priceMatch){html+='<div class="price">$'+priceMatch[1]+'<span style="font-size:0.4em;font-weight:400">'+priceMatch[2]+'</span></div>';}else{html+='<div class="subhead">'+line.slice(3)+'</div>';}}else if(line.startsWith('---')){html+='</div>';inBenefits=true;if(style==='gradient')html+='<div class="benefits-grid">';}else if(line.startsWith('- ')){const content=line.slice(2);const badgeMatch=content.match(/\\[([^\\]]+)\\]/);let title=content.split(/\\[[^\\]]+\\]/)[0].replace(/\\*\\*/g,'').trim();let desc=content.split(/\\[[^\\]]+\\]/)[1]||'';desc=desc.trim();let badgeClass='';let badgeText='';if(badgeMatch){badgeText=badgeMatch[1];const bl=badgeText.toLowerCase();if(bl.includes('free'))badgeClass='free';else if(bl.includes('earn')||badgeText.includes('$'))badgeClass='earn';else if(bl.includes('discount')||bl.includes('off'))badgeClass='discount';}html+='<div class="benefit"><span class="benefit-title">'+title+'</span>'+(badgeText?'<span class="benefit-badge '+badgeClass+'">'+badgeText+'</span>':'')+(desc?'<div class="benefit-desc">'+desc+'</div>':'')+'</div>';}else if(!line.startsWith('#')){html+='<div class="price-caption">'+line+'</div>';}}if(inBenefits&&style==='gradient')html+='</div>';html+='</div>';return html;}
function render(){const container=document.getElementById('questions');container.innerHTML='';questions.forEach((q,i)=>{const div=document.createElement('div');div.className='question'+(i===0?' active':'');div.id='q'+i;div.innerHTML=renderQuestion(q,i);container.appendChild(div);});updateProgress();attachListeners();}function renderQuestion(q,i){let html='';if(q.introContent){html+=parseIntroMarkdown(q.introContent,q.introStyle||'modern');}html+='<label class="question-label">'+q.text+'</label>';if(q.type==='single_choice'){html+='<div class="options">'+(q.options||[]).map((opt,j)=>'<div class="option"><input type="radio" name="q'+i+'" id="q'+i+'o'+j+'" value="'+escapeHtml(opt)+'"><label for="q'+i+'o'+j+'">'+escapeHtml(opt)+'</label></div>').join('')+'</div>';}else if(q.type==='multiple_choice'){html+='<div class="options">'+(q.options||[]).map((opt,j)=>'<div class="option"><input type="checkbox" name="q'+i+'" id="q'+i+'o'+j+'" value="'+escapeHtml(opt)+'"><label for="q'+i+'o'+j+'">'+escapeHtml(opt)+'</label></div>').join('')+'</div>';}else if(q.type==='text'||q.type==='long_text'){html+='<textarea id="q'+i+'text" placeholder="Your answer..."></textarea>';}html+='<div class="btn-group">';if(i>0)html+='<button class="btn btn-secondary" onclick="goBack()">Back</button>';if(i<questions.length-1)html+='<button class="btn" onclick="nextQuestion()">Continue</button>';else html+='<button class="btn" onclick="submitSurvey()">Submit</button>';html+='</div>';return html;}function escapeHtml(str){return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}function attachListeners(){document.querySelectorAll('input[type="radio"]').forEach(input=>{input.addEventListener('change',function(){const q=questions[currentQ];answers[q.id]=this.value;if(q.stripeLink&&(q.options||[]).indexOf(this.value)===0){setTimeout(()=>showStripe(q.stripeLink),300);}else{setTimeout(nextQuestion,300);}});});}function showStripe(link){const container=document.getElementById('questions');container.innerHTML='<div class="question active"><div class="stripe-box"><h3>Complete Your Subscription</h3><p>Click below to continue. Your details will be sent to <strong>'+userEmail+'</strong>.</p><a href="'+link+'" target="_blank" class="btn btn-stripe">Continue to Payment →</a></div><div class="btn-group"><button class="btn btn-secondary" onclick="location.reload()">Start Over</button></div></div>';document.querySelector('.progress').style.display='none';submitSurvey(true);}function nextQuestion(){saveCurrentAnswer();let next=currentQ+1;while(next<questions.length&&!shouldShow(questions[next]))next++;if(next>=questions.length){submitSurvey();return;}document.getElementById('q'+currentQ).classList.remove('active');currentQ=next;document.getElementById('q'+currentQ).classList.add('active');updateProgress();}function goBack(){let prev=currentQ-1;while(prev>=0&&!shouldShow(questions[prev]))prev--;if(prev<0)return;document.getElementById('q'+currentQ).classList.remove('active');currentQ=prev;document.getElementById('q'+currentQ).classList.add('active');updateProgress();}function shouldShow(q){if(!q.conditional)return true;const cond=q.conditional;const prevAnswer=answers[questions[cond.question-1]?.id];if(cond.value)return prevAnswer===cond.value;if(cond.values)return cond.values.includes(prevAnswer);return true;}function saveCurrentAnswer(){const q=questions[currentQ];if(q.type==='text'||q.type==='long_text'){const ta=document.getElementById('q'+currentQ+'text');if(ta)answers[q.id]=ta.value;}else if(q.type==='multiple_choice'){const checked=document.querySelectorAll('input[name="q'+currentQ+'"]:checked');answers[q.id]=Array.from(checked).map(c=>c.value).join(', ');}}function updateProgress(){const visible=questions.filter(shouldShow).length;const answered=questions.filter((q,i)=>i<=currentQ&&shouldShow(q)).length;document.getElementById('progressBar').style.width=Math.round((answered/visible)*100)+'%';}async function submitSurvey(partial){saveCurrentAnswer();try{await fetch('/api/surveys/'+surveyId+'/submit',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({campaign_id:campaignId,email:userEmail,first_name:userFirstName,last_name:userLastName,answers:answers})});}catch(e){console.error(e);}if(!partial){document.getElementById('questions').innerHTML='';document.querySelector('.progress').style.display='none';document.getElementById('success').classList.add('active');}}render();</script></body></html>`;
}

app.post('/api/surveys/:id/submit', async (req, res) => {
  if (!pool) return res.status(500).json({ error: 'Database not configured' });
  const { campaign_id, email, first_name, last_name, answers } = req.body;
  const responseId = generateId();
  try {
    if (email && campaign_id) { const existing = await pool.query('SELECT id FROM survey_responses WHERE email = $1 AND campaign_id = $2', [email, campaign_id]); if (existing.rows.length) return res.json({ success: true, duplicate: true }); }
    let contactId = null;
    if (hubspotClient && email) { try { const sr = await hubspotClient.crm.contacts.searchApi.doSearch({ filterGroups: [{ filters: [{ propertyName: 'email', operator: 'EQ', value: email }] }], properties: ['firstname'] }); if (sr.results.length) contactId = sr.results[0].id; } catch (e) {} }
    await pool.query('INSERT INTO survey_responses (id, survey_id, campaign_id, contact_id, email, first_name, last_name) VALUES ($1, $2, $3, $4, $5, $6, $7)', [responseId, req.params.id, campaign_id || null, contactId, email || null, first_name || null, last_name || null]);
    for (const [questionId, value] of Object.entries(answers || {})) { await pool.query('INSERT INTO survey_answers (response_id, question_id, answer_value) VALUES ($1, $2, $3)', [responseId, questionId, value]); }
    res.json({ success: true, responseId });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/campaigns/:id/survey-results', async (req, res) => {
  if (!pool) return res.json({ responses: [], stats: {} });
  try {
    const campaign = await pool.query('SELECT survey_id FROM campaigns WHERE id = $1', [req.params.id]);
    if (!campaign.rows.length || !campaign.rows[0].survey_id) return res.json({ responses: [], stats: {}, total: 0 });
    const surveyId = campaign.rows[0].survey_id;
    const questions = await pool.query('SELECT * FROM survey_questions WHERE survey_id = $1 ORDER BY order_num', [surveyId]);
    const responses = await pool.query('SELECT * FROM survey_responses WHERE campaign_id = $1 ORDER BY submitted_at DESC', [req.params.id]);
    const answers = await pool.query('SELECT sa.* FROM survey_answers sa JOIN survey_responses sr ON sa.response_id = sr.id WHERE sr.campaign_id = $1', [req.params.id]);
    const answerMap = {};
    answers.rows.forEach(a => { if (!answerMap[a.response_id]) answerMap[a.response_id] = {}; answerMap[a.response_id][a.question_id] = a.answer_value; });
    const stats = {};
    questions.rows.forEach(q => { if (!q.show_stats) return; stats[q.id] = { question: q.question_text, type: q.question_type, options: q.options || [], counts: {}, total: 0 }; (q.options || []).forEach(opt => stats[q.id].counts[opt] = 0); });
    responses.rows.forEach(r => { const ans = answerMap[r.id] || {}; Object.entries(ans).forEach(([qid, val]) => { if (stats[qid]) { stats[qid].total++; if (stats[qid].counts[val] !== undefined) stats[qid].counts[val]++; else stats[qid].counts[val] = 1; } }); });
    res.json({ total: responses.rows.length, questions: questions.rows, responses: responses.rows.map(r => ({ ...r, answers: answerMap[r.id] || {} })), stats });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Campaigns
app.get('/api/campaigns', async (req, res) => { if (!pool) return res.json([]); try { const result = await pool.query('SELECT * FROM campaigns ORDER BY created_at DESC'); res.json(result.rows.map(c => ({ ...c, contactIds: c.contact_ids ? c.contact_ids.split(',') : [], recipients: c.recipients || [], campaignType: c.campaign_type || 'email', surveyId: c.survey_id }))); } catch (e) { res.status(500).json({ error: e.message }); } });
app.get('/api/campaigns/:id', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); try { const result = await pool.query('SELECT * FROM campaigns WHERE id = $1', [req.params.id]); if (!result.rows.length) return res.status(404).json({ error: 'Not found' }); const c = result.rows[0]; res.json({ ...c, contactIds: c.contact_ids ? c.contact_ids.split(',') : [], recipients: c.recipients || [], campaignType: c.campaign_type || 'email', surveyId: c.survey_id }); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/api/campaigns', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); const { name, subject, body, contactIds, campaignType, surveyId } = req.body; const id = Date.now().toString(); const trackingId = generateTrackingId(); try { await pool.query('INSERT INTO campaigns (id, name, subject, body, status, campaign_type, survey_id, contact_ids, tracking_id, recipients, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())', [id, name || 'Untitled', subject || '', body || '', 'draft', campaignType || 'email', surveyId || null, (contactIds || []).join(','), trackingId, JSON.stringify([])]); res.json({ id, name: name || 'Untitled', subject, body, status: 'draft', contactIds: contactIds || [], trackingId, recipients: [], createdAt: new Date().toISOString(), campaignType: campaignType || 'email', surveyId }); } catch (e) { res.status(500).json({ error: e.message }); } });
app.put('/api/campaigns/:id', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); const { name, subject, body, contactIds, scheduledTime, campaignType, surveyId } = req.body; try { const current = await pool.query('SELECT status FROM campaigns WHERE id = $1', [req.params.id]); if (!current.rows.length) return res.status(404).json({ error: 'Not found' }); if (current.rows[0].status === 'sent') return res.status(400).json({ error: 'Cannot edit sent campaign' }); await pool.query('UPDATE campaigns SET name = COALESCE($1, name), subject = COALESCE($2, subject), body = COALESCE($3, body), contact_ids = COALESCE($4, contact_ids), scheduled_time = $5, campaign_type = COALESCE($6, campaign_type), survey_id = $7 WHERE id = $8', [name, subject, body, contactIds ? contactIds.join(',') : null, scheduledTime, campaignType, surveyId, req.params.id]); res.json({ success: true }); } catch (e) { res.status(500).json({ error: e.message }); } });
app.delete('/api/campaigns/:id', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); try { const current = await pool.query('SELECT status FROM campaigns WHERE id = $1', [req.params.id]); if (current.rows.length && current.rows[0].status === 'sent') return res.status(400).json({ error: 'Cannot delete sent campaign' }); await pool.query('DELETE FROM campaigns WHERE id = $1', [req.params.id]); res.json({ success: true }); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/api/campaigns/:id/duplicate', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); try { const orig = await pool.query('SELECT * FROM campaigns WHERE id = $1', [req.params.id]); if (!orig.rows.length) return res.status(404).json({ error: 'Not found' }); const o = orig.rows[0]; const id = Date.now().toString(); const trackingId = generateTrackingId(); await pool.query('INSERT INTO campaigns (id, name, subject, body, status, campaign_type, survey_id, contact_ids, tracking_id, recipients, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())', [id, o.name + ' (Copy)', o.subject, o.body, 'draft', o.campaign_type || 'email', o.survey_id, o.contact_ids, trackingId, JSON.stringify([])]); res.json({ id, name: o.name + ' (Copy)', subject: o.subject, body: o.body, status: 'draft', contactIds: o.contact_ids ? o.contact_ids.split(',') : [], trackingId, recipients: [], createdAt: new Date().toISOString(), campaignType: o.campaign_type || 'email', surveyId: o.survey_id }); } catch (e) { res.status(500).json({ error: e.message }); } });

function injectTracking(body, trackingId) { const hidden = '<span style="display:none!important;font-size:0;color:transparent">CID:' + trackingId + '</span>'; const footer = '<p style="color:#999;font-size:12px;margin-top:30px">PM CoE Team | Ref #' + trackingId + '</p>'; if (body.includes('</body>')) return body.replace('</body>', footer + hidden + '</body>'); return body + footer + hidden; }
function wrapEmailHTML(body) { return '<!DOCTYPE html><html><head><meta charset="utf-8"><style>body{font-family:Arial,sans-serif;font-size:15px;line-height:1.6;color:#333;max-width:600px;margin:0 auto;padding:20px}p{margin-bottom:15px}ul,ol{margin-bottom:15px;padding-left:20px}li{margin-bottom:8px}a{color:#e94560}</style></head><body>' + body + '</body></html>'; }
async function updateHubSpotContact(contactId) { if (!hubspotClient) return; try { const c = await hubspotClient.crm.contacts.basicApi.getById(contactId, ['email_campaign_count']); const count = parseInt(c.properties.email_campaign_count) || 0; await hubspotClient.crm.contacts.basicApi.update(contactId, { properties: { last_email_sent_date: new Date().toISOString().split('T')[0], email_campaign_count: (count + 1).toString() } }); } catch (e) {} }

app.post('/api/campaigns/:id/send', async (req, res) => {
  if (!hubspotClient || !sgInitialized || !pool) return res.status(500).json({ error: 'Services not configured' });
  try {
    const campResult = await pool.query('SELECT * FROM campaigns WHERE id = $1', [req.params.id]);
    if (!campResult.rows.length) return res.status(404).json({ error: 'Not found' });
    const campaign = campResult.rows[0];
    const contactIds = campaign.contact_ids ? campaign.contact_ids.split(',').filter(x => x) : [];
    if (!contactIds.length) return res.status(400).json({ error: 'No recipients' });
    const contacts = await hubspotClient.crm.contacts.batchApi.read({ inputs: contactIds.map(id => ({ id })), properties: ['firstname', 'lastname', 'email'] });
    const recipients = [];
    const results = { sent: 0, failed: 0 };
    const surveyBaseUrl = campaign.survey_id ? (process.env.APP_URL || 'https://' + req.get('host')) + '/survey/' + campaign.survey_id : null;
    for (const contact of contacts.results) {
      const email = contact.properties.email, firstName = contact.properties.firstname || '', lastName = contact.properties.lastname || '';
      const recipient = { contactId: contact.id, name: (firstName + ' ' + lastName).trim(), email, sent: false, delivered: false, opened: false, clicked: false, bounced: false, error: null };
      if (!email) { results.failed++; recipient.error = 'No email'; recipients.push(recipient); continue; }
      let emailBody = campaign.body.replace(/\[First Name\]/gi, firstName);
      if (surveyBaseUrl) {
        const surveyLink = surveyBaseUrl + '?email=' + encodeURIComponent(email) + '&fname=' + encodeURIComponent(firstName) + '&lname=' + encodeURIComponent(lastName) + '&campaign_id=' + campaign.id;
        emailBody = emailBody.replace(/\{\{survey_link\}\}/gi, surveyLink);
        emailBody = emailBody.replace(/\[Survey Link\]/gi, '<a href="' + surveyLink + '" style="display:inline-block;padding:12px 24px;background:#e94560;color:white;text-decoration:none;border-radius:6px;font-weight:500">Take the Survey</a>');
      }
      const wrappedBody = wrapEmailHTML(emailBody);
      const finalBody = injectTracking(wrappedBody, campaign.tracking_id);
      try {
        await sgMail.send({ to: email, from: process.env.FROM_EMAIL || 'noreply@example.com', subject: campaign.subject, html: finalBody, trackingSettings: { clickTracking: { enable: true }, openTracking: { enable: true } }, customArgs: { campaignId: campaign.id, contactId: contact.id } });
        recipient.sent = true; results.sent++;
        updateHubSpotContact(contact.id);
      } catch (e) { results.failed++; recipient.error = e.message; }
      recipients.push(recipient);
    }
    await pool.query('UPDATE campaigns SET status = $1, sent_at = NOW(), recipients = $2 WHERE id = $3', ['sent', JSON.stringify(recipients), req.params.id]);
    res.json(results);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/campaigns/:id/schedule', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); const { scheduledTime } = req.body; if (!scheduledTime) return res.status(400).json({ error: 'Schedule time required' }); try { await pool.query('UPDATE campaigns SET status = $1, scheduled_time = $2 WHERE id = $3', ['scheduled', scheduledTime, req.params.id]); res.json({ success: true }); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/api/campaigns/:id/cancel', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); try { await pool.query('UPDATE campaigns SET status = $1, scheduled_time = NULL WHERE id = $2', ['draft', req.params.id]); res.json({ success: true }); } catch (e) { res.status(500).json({ error: e.message }); } });

app.post('/api/webhooks/sendgrid', async (req, res) => { if (!pool) return res.sendStatus(200); const events = req.body; if (!Array.isArray(events)) return res.sendStatus(200); for (const ev of events) { const { campaignId, contactId } = ev; if (!campaignId || !contactId) continue; try { const result = await pool.query('SELECT recipients FROM campaigns WHERE id = $1', [campaignId]); if (!result.rows.length) continue; const recipients = result.rows[0].recipients || []; const recipient = recipients.find(r => r.contactId === contactId); if (!recipient) continue; if (ev.event === 'delivered') recipient.delivered = true; else if (ev.event === 'open') recipient.opened = true; else if (ev.event === 'click') recipient.clicked = true; else if (ev.event === 'bounce' || ev.event === 'dropped') { recipient.bounced = true; recipient.delivered = false; } await pool.query('UPDATE campaigns SET recipients = $1 WHERE id = $2', [JSON.stringify(recipients), campaignId]); } catch (e) {} } res.sendStatus(200); });

// Reply classification
async function classifyReply(text) { if (!anthropic) return { interests: [], category: 'unclear', summary: 'AI not configured', suggestedAction: 'Manual review', requiresReview: true }; try { const r = await anthropic.messages.create({ model: 'claude-sonnet-4-20250514', max_tokens: 1000, messages: [{ role: 'user', content: 'Analyze this email reply. Return ONLY JSON (no markdown):\n{"interests":["PDUs","Referral Program","Career Coaching","Job Connections","Next Certification"],"category":"interested","summary":"...","suggestedAction":"...","requiresReview":false}\ninterests: services mentioned\ncategory: interested|question|not_interested|out_of_office|unclear\nrequiresReview: true if question/unclear\n\nReply: "' + text + '"' }] }); let t = r.content[0].text.trim().replace(/```json\n?/g, '').replace(/```\n?/g, ''); return JSON.parse(t); } catch (e) { return { interests: [], category: 'unclear', summary: 'Classification failed', suggestedAction: 'Manual review', requiresReview: true }; } }
async function findCampaignByTracking(body) { if (!pool) return null; const match = body.match(/CID:([A-F0-9]{8})/i) || body.match(/Ref #([A-F0-9]{8})/i); if (match) { const result = await pool.query('SELECT id FROM campaigns WHERE tracking_id = $1', [match[1]]); if (result.rows.length) return result.rows[0].id; } return null; }

async function checkForReplies() {
  console.log('checkForReplies called');
  console.log('gmailTokens:', gmailTokens ? 'EXISTS' : 'NULL');
  console.log('oauth2Client:', oauth2Client ? 'EXISTS' : 'NULL');
  console.log('pool:', pool ? 'EXISTS' : 'NULL');
  if (!gmailTokens || !oauth2Client || !pool) {
    console.log('Early return - missing:', !gmailTokens ? 'gmailTokens' : '', !oauth2Client ? 'oauth2Client' : '', !pool ? 'pool' : '');
    return;
  }
  try {
    oauth2Client.setCredentials(gmailTokens);
    const gmail = google.gmail({ version: 'v1', auth: oauth2Client });
    console.log('Fetching Gmail messages...');
    // Fetch recent inbox messages (not just unread) - we track processed ones in DB
    const list = await gmail.users.messages.list({ userId: 'me', q: 'in:inbox', maxResults: 20 });
    console.log('Gmail response:', list.data.messages ? list.data.messages.length + ' messages' : 'no messages');
    if (!list.data.messages) {
      console.log('No messages found in inbox');
      return;
    }
    for (const m of list.data.messages) {
      console.log('Processing message:', m.id);
      if (processedMessageIds.has(m.id)) {
        console.log('  - Already in processedMessageIds, skipping');
        continue;
      }
      const existing = await pool.query('SELECT id FROM replies WHERE id = $1', [m.id]);
      if (existing.rows.length) { 
        console.log('  - Already in DB, skipping');
        processedMessageIds.add(m.id); 
        continue; 
      }
      console.log('  - Fetching full message...');
      const msg = await gmail.users.messages.get({ userId: 'me', id: m.id, format: 'full' });
      const headers = msg.data.payload.headers;
      const from = headers.find(h => h.name === 'From')?.value || '';
      const subject = headers.find(h => h.name === 'Subject')?.value || '';
      console.log('  - From:', from);
      console.log('  - Subject:', subject);
      let body = '';
      // Extract body - try multiple methods
      if (msg.data.payload.body?.data) {
        body = Buffer.from(msg.data.payload.body.data, 'base64').toString();
      } else if (msg.data.payload.parts) {
        // Try text/plain first, then text/html
        let part = msg.data.payload.parts.find(x => x.mimeType === 'text/plain');
        if (!part) part = msg.data.payload.parts.find(x => x.mimeType === 'text/html');
        // Check nested parts (for multipart/alternative)
        if (!part) {
          for (const p of msg.data.payload.parts) {
            if (p.parts) {
              part = p.parts.find(x => x.mimeType === 'text/plain') || p.parts.find(x => x.mimeType === 'text/html');
              if (part) break;
            }
          }
        }
        if (part?.body?.data) body = Buffer.from(part.body.data, 'base64').toString();
      }
      // Strip HTML tags if we got HTML content
      if (body.includes('<html') || body.includes('<div') || body.includes('<p>')) {
        body = body.replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
      }
      console.log('  - Body length:', body.length);
      console.log('  - Body preview:', body.substring(0, 100));
      const emailMatch = from.match(/<(.+)>/) || [null, from];
      const senderEmail = emailMatch[1];
      console.log('  - Sender email:', senderEmail);
      const campaignId = await findCampaignByTracking(body);
      console.log('  - Campaign ID from tracking:', campaignId);
      let contact = null;
      if (hubspotClient) { try { const sr = await hubspotClient.crm.contacts.searchApi.doSearch({ filterGroups: [{ filters: [{ propertyName: 'email', operator: 'EQ', value: senderEmail }] }], properties: ['firstname', 'lastname'] }); if (sr.results.length) contact = sr.results[0]; } catch (e) {} }
      const classification = await classifyReply(body);
      console.log('  - Classification:', classification.category);
      const status = classification.requiresReview ? 'pending_review' : 'ready_to_send';
      console.log('  - Inserting into replies table...');
      await pool.query('INSERT INTO replies (id, campaign_id, from_email, from_name, subject, body, classification, status, contact_id, contact_name, received_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())', [m.id, campaignId, senderEmail, from.replace(/<.+>/, '').trim(), subject, body.substring(0, 1000), JSON.stringify(classification), status, contact?.id || null, contact ? (contact.properties.firstname || '') + ' ' + (contact.properties.lastname || '') : null]);
      console.log('  - Successfully inserted reply:', m.id);
      processedMessageIds.add(m.id);
      await pool.query('INSERT INTO processed_messages (message_id) VALUES ($1) ON CONFLICT DO NOTHING', [m.id]);
    }
    console.log('checkForReplies completed');
  } catch (e) { console.error('Gmail check error:', e.message, e.stack); }
}

app.get('/api/replies', async (req, res) => { if (!pool) return res.json([]); try { const result = await pool.query('SELECT * FROM replies ORDER BY received_at DESC'); res.json(result.rows.map(r => ({ ...r, from: r.from_email, fromName: r.from_name, contactId: r.contact_id, contactName: r.contact_name, campaignId: r.campaign_id, receivedAt: r.received_at, followUpSent: r.follow_up_sent }))); } catch (e) { res.status(500).json({ error: e.message }); } });
app.get('/api/replies/unlinked', async (req, res) => { if (!pool) return res.json([]); try { const result = await pool.query('SELECT * FROM replies WHERE campaign_id IS NULL ORDER BY received_at DESC'); res.json(result.rows.map(r => ({ ...r, from: r.from_email, fromName: r.from_name, contactId: r.contact_id, contactName: r.contact_name, campaignId: r.campaign_id, receivedAt: r.received_at, followUpSent: r.follow_up_sent }))); } catch (e) { res.status(500).json({ error: e.message }); } });
app.get('/api/campaigns/:id/replies', async (req, res) => { if (!pool) return res.json([]); try { const result = await pool.query('SELECT * FROM replies WHERE campaign_id = $1 ORDER BY received_at DESC', [req.params.id]); res.json(result.rows.map(r => ({ ...r, from: r.from_email, fromName: r.from_name, contactId: r.contact_id, contactName: r.contact_name, campaignId: r.campaign_id, receivedAt: r.received_at, followUpSent: r.follow_up_sent }))); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/api/replies/check', async (req, res) => { await checkForReplies(); res.json({ success: true }); });
app.post('/api/replies/:id/respond', async (req, res) => { if (!sgInitialized || !pool) return res.status(500).json({ error: 'Services not configured' }); try { const result = await pool.query('SELECT * FROM replies WHERE id = $1', [req.params.id]); if (!result.rows.length) return res.status(404).json({ error: 'Not found' }); const reply = result.rows[0]; const { responseBody } = req.body; await sgMail.send({ to: reply.from_email, from: process.env.FROM_EMAIL || 'noreply@example.com', subject: 'Re: ' + reply.subject, html: responseBody }); await pool.query('UPDATE replies SET status = $1, follow_up_sent = TRUE WHERE id = $2', ['responded', req.params.id]); res.json({ success: true }); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/api/replies/:id/dismiss', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); try { await pool.query('UPDATE replies SET status = $1 WHERE id = $2', ['dismissed', req.params.id]); res.json({ success: true }); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/api/replies/:id/link', async (req, res) => { if (!pool) return res.status(500).json({ error: 'Database not configured' }); try { await pool.query('UPDATE replies SET campaign_id = $1 WHERE id = $2', [req.body.campaignId, req.params.id]); res.json({ success: true }); } catch (e) { res.status(500).json({ error: e.message }); } });

// ==================== LEAD CAPTURE ENGINE ====================

function parsePromotionEmail(subject, body, toEmail) {
  // Extract course from subject: "PMP promotion code claim", "CAPM promotion code claim", etc.
  const courseMatch = subject.match(/^(PMP|CAPM|PMI-CP|PMI-ACP)\s+promotion\s+code\s+claim/i);
  if (!courseMatch) return null;
  const course = courseMatch[1].toUpperCase();
  
  // Extract name: "Hi Santiago Verguizas Sanchez," or "Hi Atulya Andotra,"
  const nameMatch = body.match(/Hi\s+([^,]+),/i);
  let firstName = '', lastName = '';
  if (nameMatch) {
    const parts = nameMatch[1].trim().split(/\s+/);
    firstName = parts[0] || '';
    lastName = parts.slice(1).join(' ') || '';
  }
  
  // Extract promo code: "promotion code is SPECIAL60" or "promotion code is NFP"
  const promoMatch = body.match(/promotion\s+code\s+(?:is\s+)?(\w+)/i);
  const promoCode = promoMatch ? promoMatch[1] : '';
  
  // Extract enrollment URL
  const urlMatch = body.match(/https?:\/\/www\.pm-coe\.com\/[^\s"<]+/i);
  const enrollmentUrl = urlMatch ? urlMatch[0] : '';
  
  // Extract region from URL or body text
  let region = 'Unknown';
  if (enrollmentUrl.includes('_na') || body.toLowerCase().includes('north america')) region = 'North America';
  else if (body.toLowerCase().includes('europe') || enrollmentUrl.includes('europe')) region = 'Europe';
  else if (body.toLowerCase().includes('australia') || enrollmentUrl.includes('_au')) region = 'Australia';
  
  return { course, firstName, lastName, email: toEmail, promoCode, enrollmentUrl, region };
}

function scoreLead(lead) {
  let score = 50; // Base score for downloading a discount code
  // Course value
  if (lead.course === 'PMP') score += 20;
  else if (lead.course === 'PMI-CP') score += 15;
  else if (lead.course === 'CAPM') score += 10;
  // Corporate email (not gmail/yahoo/hotmail/outlook)
  const freeProviders = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com', 'icloud.com', 'live.com', 'mail.com', 'protonmail.com'];
  const domain = lead.email.split('@')[1]?.toLowerCase();
  if (domain && !freeProviders.includes(domain)) score += 20; // Corporate email
  // Region
  if (lead.region === 'North America') score += 5;
  return Math.min(score, 100);
}

async function checkForLeads() {
  if (!gmailTokens || !oauth2Client || !pool) return;
  try {
    oauth2Client.setCredentials(gmailTokens);
    const gmail = google.gmail({ version: 'v1', auth: oauth2Client });
    // Search sent folder for promotion code claim emails
    const list = await gmail.users.messages.list({ 
      userId: 'me', 
      q: 'in:sent subject:"promotion code claim"', 
      maxResults: 20 
    });
    if (!list.data.messages) return;
    
    let newLeadsCount = 0;
    for (const m of list.data.messages) {
      if (leadProcessedIds.has(m.id)) continue;
      
      // Check DB too
      const existing = await pool.query('SELECT message_id FROM lead_processed_emails WHERE message_id = $1', [m.id]);
      if (existing.rows.length) { leadProcessedIds.add(m.id); continue; }
      
      const msg = await gmail.users.messages.get({ userId: 'me', id: m.id, format: 'full' });
      const headers = msg.data.payload.headers;
      const subject = headers.find(h => h.name === 'Subject')?.value || '';
      const to = headers.find(h => h.name === 'To')?.value || '';
      
      // Extract recipient email
      const toMatch = to.match(/<(.+)>/) || to.match(/([^\s,]+@[^\s,]+)/);
      const toEmail = toMatch ? toMatch[1] : to.trim();
      
      // Skip if sent to self
      if (toEmail.toLowerCase().includes('alan.k@pm-coe.com')) {
        leadProcessedIds.add(m.id);
        await pool.query('INSERT INTO lead_processed_emails (message_id) VALUES ($1) ON CONFLICT DO NOTHING', [m.id]);
        continue;
      }
      
      // Extract body
      let body = '';
      if (msg.data.payload.body?.data) {
        body = Buffer.from(msg.data.payload.body.data, 'base64').toString();
      } else if (msg.data.payload.parts) {
        let part = msg.data.payload.parts.find(x => x.mimeType === 'text/plain');
        if (!part) part = msg.data.payload.parts.find(x => x.mimeType === 'text/html');
        if (!part) {
          for (const p of msg.data.payload.parts) {
            if (p.parts) { part = p.parts.find(x => x.mimeType === 'text/plain') || p.parts.find(x => x.mimeType === 'text/html'); if (part) break; }
          }
        }
        if (part?.body?.data) body = Buffer.from(part.body.data, 'base64').toString();
      }
      if (body.includes('<html') || body.includes('<div') || body.includes('<p>')) {
        body = body.replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
      }
      
      const lead = parsePromotionEmail(subject, body, toEmail);
      if (!lead) {
        leadProcessedIds.add(m.id);
        await pool.query('INSERT INTO lead_processed_emails (message_id) VALUES ($1) ON CONFLICT DO NOTHING', [m.id]);
        continue;
      }
      
      lead.score = scoreLead(lead);
      const leadId = generateId();
      
      // Insert or update lead (same person might download multiple course codes)
      try {
        await pool.query(`INSERT INTO leads (id, email, first_name, last_name, course, region, promo_code, enrollment_url, score, gmail_message_id, captured_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
          ON CONFLICT (email, course) DO UPDATE SET promo_code = $7, enrollment_url = $8, score = GREATEST(leads.score, $9), captured_at = NOW()`,
          [leadId, lead.email, lead.firstName, lead.lastName, lead.course, lead.region, lead.promoCode, lead.enrollmentUrl, lead.score, m.id]);
      } catch (e) { console.error('Lead insert error:', e.message); }
      
      // Add to HubSpot
      if (hubspotClient) {
        try {
          const sr = await hubspotClient.crm.contacts.searchApi.doSearch({
            filterGroups: [{ filters: [{ propertyName: 'email', operator: 'EQ', value: lead.email }] }],
            properties: ['customer_tag']
          });
          const tag = `lead-${lead.course.toLowerCase()}`;
          if (sr.results.length) {
            const ex = sr.results[0];
            let tags = (ex.properties.customer_tag || '').split(',').map(t => t.trim()).filter(t => t);
            if (!tags.includes(tag)) tags.push(tag);
            if (!tags.includes('auto-captured')) tags.push('auto-captured');
            await hubspotClient.crm.contacts.basicApi.update(ex.id, { properties: { customer_tag: tags.join(',') } });
            await pool.query('UPDATE leads SET hubspot_contact_id = $1 WHERE email = $2 AND course = $3', [ex.id, lead.email, lead.course]);
          } else {
            const created = await hubspotClient.crm.contacts.basicApi.create({ properties: {
              email: lead.email, firstname: lead.firstName, lastname: lead.lastName,
              customer_tag: `${tag},auto-captured`
            }});
            await pool.query('UPDATE leads SET hubspot_contact_id = $1 WHERE email = $2 AND course = $3', [created.id, lead.email, lead.course]);
          }
        } catch (e) { console.error('HubSpot lead sync error:', e.message); }
      }
      
      // Create nurture sequence
      try {
        // Get actual lead ID (might have been ON CONFLICT updated)
        const leadRow = await pool.query('SELECT id FROM leads WHERE email = $1 AND course = $2', [lead.email, lead.course]);
        const actualLeadId = leadRow.rows[0]?.id;
        if (actualLeadId) {
          // Check if sequence already exists
          const existingSeq = await pool.query('SELECT id FROM lead_sequences WHERE lead_id = $1', [actualLeadId]);
          if (existingSeq.rows.length === 0) {
            const configs = await pool.query('SELECT * FROM lead_sequence_configs WHERE course = $1 AND enabled = TRUE ORDER BY step', [lead.course]);
            for (const config of configs.rows) {
              let emailSubject = config.subject.replace(/\[First Name\]/gi, lead.firstName);
              let emailBody = config.body
                .replace(/\[First Name\]/gi, lead.firstName)
                .replace(/\[Promo Code\]/gi, lead.promoCode)
                .replace(/\[Enrollment URL\]/gi, lead.enrollmentUrl);
              const scheduledFor = new Date(Date.now() + config.delay_days * 24 * 60 * 60 * 1000);
              await pool.query('INSERT INTO lead_sequences (lead_id, step, subject, body, scheduled_for) VALUES ($1, $2, $3, $4, $5)',
                [actualLeadId, config.step, emailSubject, emailBody, scheduledFor]);
            }
          }
        }
      } catch (e) { console.error('Sequence creation error:', e.message); }
      
      leadProcessedIds.add(m.id);
      await pool.query('INSERT INTO lead_processed_emails (message_id) VALUES ($1) ON CONFLICT DO NOTHING', [m.id]);
      newLeadsCount++;
      console.log(`Lead captured: ${lead.firstName} ${lead.lastName} <${lead.email}> - ${lead.course} (score: ${lead.score})`);
    }
    if (newLeadsCount > 0) console.log(`Lead scan complete: ${newLeadsCount} new leads captured`);
  } catch (e) { console.error('Lead capture error:', e.message, e.stack); }
}

async function processLeadSequences() {
  if (!pool || !sgInitialized) return;
  try {
    const due = await pool.query(`SELECT ls.*, l.email, l.first_name, l.enrollment_url, l.promo_code, l.course
      FROM lead_sequences ls JOIN leads l ON ls.lead_id = l.id
      WHERE ls.status = 'pending' AND ls.scheduled_for <= NOW() AND l.status != 'converted' AND l.status != 'unsubscribed'
      ORDER BY ls.scheduled_for`);
    
    for (const seq of due.rows) {
      try {
        const wrappedBody = wrapEmailHTML(seq.body);
        await sgMail.send({
          to: seq.email,
          from: process.env.FROM_EMAIL || 'noreply@example.com',
          subject: seq.subject,
          html: wrappedBody,
          trackingSettings: { clickTracking: { enable: true }, openTracking: { enable: true } },
          customArgs: { leadSequence: seq.id.toString() }
        });
        await pool.query('UPDATE lead_sequences SET status = $1, sent_at = NOW() WHERE id = $2', ['sent', seq.id]);
        console.log(`Lead nurture sent: Step ${seq.step} to ${seq.email} (${seq.course})`);
      } catch (e) {
        await pool.query('UPDATE lead_sequences SET status = $1 WHERE id = $2', ['failed', seq.id]);
        console.error(`Lead nurture failed: ${seq.email}`, e.message);
      }
    }
  } catch (e) { console.error('Lead sequence processor error:', e.message); }
}

// ==================== LEAD API ENDPOINTS ====================

// Get all leads with stats
app.get('/api/leads', async (req, res) => {
  if (!pool) return res.json([]);
  try {
    const result = await pool.query(`SELECT l.*, 
      (SELECT COUNT(*) FROM lead_sequences ls WHERE ls.lead_id = l.id AND ls.status = 'sent') as emails_sent,
      (SELECT COUNT(*) FROM lead_sequences ls WHERE ls.lead_id = l.id AND ls.opened = TRUE) as emails_opened,
      (SELECT COUNT(*) FROM lead_sequences ls WHERE ls.lead_id = l.id AND ls.clicked = TRUE) as emails_clicked,
      (SELECT MAX(ls.step) FROM lead_sequences ls WHERE ls.lead_id = l.id AND ls.status = 'sent') as last_step_sent,
      (SELECT COUNT(*) FROM lead_sequences ls WHERE ls.lead_id = l.id AND ls.status = 'pending') as emails_pending
      FROM leads l ORDER BY l.captured_at DESC`);
    res.json(result.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Get lead stats summary
app.get('/api/leads/stats', async (req, res) => {
  if (!pool) return res.json({});
  try {
    const total = await pool.query('SELECT COUNT(*) FROM leads');
    const byStatus = await pool.query('SELECT status, COUNT(*) FROM leads GROUP BY status');
    const byCourse = await pool.query('SELECT course, COUNT(*) FROM leads GROUP BY course');
    const thisWeek = await pool.query("SELECT COUNT(*) FROM leads WHERE captured_at >= NOW() - INTERVAL '7 days'");
    const hotLeads = await pool.query('SELECT COUNT(*) FROM leads WHERE score >= 80');
    const sequencesSent = await pool.query("SELECT COUNT(*) FROM lead_sequences WHERE status = 'sent'");
    res.json({
      total: parseInt(total.rows[0].count),
      thisWeek: parseInt(thisWeek.rows[0].count),
      hot: parseInt(hotLeads.rows[0].count),
      sequencesSent: parseInt(sequencesSent.rows[0].count),
      byStatus: Object.fromEntries(byStatus.rows.map(r => [r.status, parseInt(r.count)])),
      byCourse: Object.fromEntries(byCourse.rows.map(r => [r.course, parseInt(r.count)]))
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Get lead sequence details
app.get('/api/leads/:id/sequence', async (req, res) => {
  if (!pool) return res.json([]);
  try {
    const result = await pool.query('SELECT * FROM lead_sequences WHERE lead_id = $1 ORDER BY step', [req.params.id]);
    res.json(result.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Update lead status
app.put('/api/leads/:id/status', async (req, res) => {
  if (!pool) return res.status(500).json({ error: 'Database not configured' });
  try {
    const { status } = req.body;
    const updates = status === 'converted' ? 'status = $1, converted_at = NOW()' : 'status = $1';
    await pool.query(`UPDATE leads SET ${updates} WHERE id = $2`, [status, req.params.id]);
    if (status === 'unsubscribed') {
      await pool.query("UPDATE lead_sequences SET status = 'cancelled' WHERE lead_id = $1 AND status = 'pending'", [req.params.id]);
    }
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Manually trigger lead scan
app.post('/api/leads/scan', async (req, res) => {
  await checkForLeads();
  res.json({ success: true });
});

// Get/update sequence configs
app.get('/api/leads/sequences/config', async (req, res) => {
  if (!pool) return res.json([]);
  try {
    const result = await pool.query('SELECT * FROM lead_sequence_configs ORDER BY course, step');
    res.json(result.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/leads/sequences/config/:id', async (req, res) => {
  if (!pool) return res.status(500).json({ error: 'Database not configured' });
  try {
    const { delay_days, subject, body, enabled } = req.body;
    await pool.query('UPDATE lead_sequence_configs SET delay_days = $1, subject = $2, body = $3, enabled = $4 WHERE id = $5',
      [delay_days, subject, body, enabled, req.params.id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Add new course sequence config
app.post('/api/leads/sequences/config', async (req, res) => {
  if (!pool) return res.status(500).json({ error: 'Database not configured' });
  try {
    const { course, step, delay_days, subject, body } = req.body;
    const result = await pool.query('INSERT INTO lead_sequence_configs (course, step, delay_days, subject, body) VALUES ($1, $2, $3, $4, $5) RETURNING *',
      [course, step, delay_days, subject, body]);
    res.json(result.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ==================== END LEAD CAPTURE ====================

// File parsing
function parseExcel(buffer) { const workbook = XLSX.read(buffer, { type: 'buffer' }); const sheet = workbook.Sheets[workbook.SheetNames[0]]; return XLSX.utils.sheet_to_json(sheet, { header: 1 }); }
function parseRowsToContacts(rows) { if (!rows || rows.length === 0) return []; const firstRow = rows[0]; const hasHeader = firstRow && firstRow.some(cell => { const c = String(cell || '').toLowerCase(); return c.includes('name') || c.includes('email') || c.includes('first') || c.includes('last') || c.includes('company'); }); let headers = null, dataRows = rows; if (hasHeader) { headers = firstRow.map(h => String(h || '').toLowerCase()); dataRows = rows.slice(1); } const contacts = []; for (const row of dataRows) { if (!row || row.length === 0 || row.every(c => !c)) continue; let firstName = '', lastName = '', email = '', company = '', phone = '', jobTitle = ''; if (headers) { const fi = headers.findIndex(h => h.includes('first')), li = headers.findIndex(h => h.includes('last')), ei = headers.findIndex(h => h.includes('email')), ci = headers.findIndex(h => h.includes('company') || h.includes('org')), pi = headers.findIndex(h => h.includes('phone')), ji = headers.findIndex(h => h.includes('title') || h.includes('job')); if (fi >= 0) firstName = String(row[fi] || '').trim(); if (li >= 0) lastName = String(row[li] || '').trim(); if (ei >= 0) email = String(row[ei] || '').trim(); if (ci >= 0) company = String(row[ci] || '').trim(); if (pi >= 0) phone = String(row[pi] || '').trim(); if (ji >= 0) jobTitle = String(row[ji] || '').trim(); if (!email) { for (const cell of row) { const s = String(cell || ''); if (s.includes('@') && s.includes('.')) { email = s.trim(); break; } } } } else { for (const cell of row) { const s = String(cell || '').trim(); if (!email && s.includes('@') && s.includes('.')) email = s; else if (!firstName && s && !s.includes('@')) firstName = s; else if (!lastName && s && !s.includes('@') && firstName) lastName = s; } } contacts.push({ firstName, lastName, email, company, phone, jobTitle, valid: email && email.includes('@') && email.includes('.') }); } return contacts; }
async function parseFile(filePath, ext) { const buffer = fs.readFileSync(filePath); if (ext === '.xlsx' || ext === '.xls') return { type: 'structured', contacts: parseRowsToContacts(parseExcel(buffer)) }; if (ext === '.csv') return { type: 'structured', contacts: parseRowsToContacts(buffer.toString('utf-8').split('\n').map(line => line.split(',').map(c => c.trim().replace(/^["']|["']$/g, '')))) }; if (ext === '.pdf') { try { return { type: 'text', content: (await pdfParse(buffer)).text }; } catch (e) { return { type: 'error', error: 'Failed to parse PDF' }; } } if (ext === '.docx') { try { return { type: 'text', content: (await mammoth.extractRawText({ buffer })).value }; } catch (e) { return { type: 'error', error: 'Failed to parse Word doc' }; } } if (['.png', '.jpg', '.jpeg', '.gif', '.webp'].includes(ext)) return { type: 'image', buffer }; return { type: 'text', content: buffer.toString('utf-8') }; }
app.post('/api/import/extract', upload.single('file'), async (req, res) => { try { if (!req.file) return res.status(400).json({ error: 'No file' }); const ext = path.extname(req.file.originalname).toLowerCase(); const parsed = await parseFile(req.file.path, ext); fs.unlinkSync(req.file.path); if (parsed.type === 'error') return res.status(400).json({ error: parsed.error }); if (parsed.type === 'structured') return res.json({ contacts: parsed.contacts }); if (!anthropic) return res.status(500).json({ error: 'AI not configured' }); let content; if (parsed.type === 'image') { const mt = ext === '.png' ? 'image/png' : ext === '.gif' ? 'image/gif' : ext === '.webp' ? 'image/webp' : 'image/jpeg'; const r = await anthropic.messages.create({ model: 'claude-sonnet-4-20250514', max_tokens: 4000, messages: [{ role: 'user', content: [{ type: 'image', source: { type: 'base64', media_type: mt, data: parsed.buffer.toString('base64') } }, { type: 'text', text: DEFAULT_IMPORT_PROMPT }] }] }); content = r.content[0].text; } else { const r = await anthropic.messages.create({ model: 'claude-sonnet-4-20250514', max_tokens: 4000, messages: [{ role: 'user', content: DEFAULT_IMPORT_PROMPT + '\n\nText:\n' + parsed.content }] }); content = r.content[0].text; } const m = content.match(/\[[\s\S]*\]/); res.json({ contacts: m ? JSON.parse(m[0]) : [] }); } catch (e) { res.status(500).json({ error: e.message }); } });
app.post('/api/import/confirm', async (req, res) => { if (!hubspotClient) return res.status(500).json({ error: 'HubSpot not configured' }); try { const { contacts, tags } = req.body; if (pool && tags?.length) { for (const tag of tags) { try { await pool.query('INSERT INTO tags (name) VALUES ($1) ON CONFLICT DO NOTHING', [tag]); } catch (e) {} } } const results = { created: 0, updated: 0, skipped: 0 }; for (const c of contacts) { if (!c.email || !c.valid) { results.skipped++; continue; } try { const sr = await hubspotClient.crm.contacts.searchApi.doSearch({ filterGroups: [{ filters: [{ propertyName: 'email', operator: 'EQ', value: c.email }] }], properties: ['customer_tag'] }); const props = { email: c.email, firstname: c.firstName || '', lastname: c.lastName || '', company: c.company || '', phone: c.phone || '', jobtitle: c.jobTitle || '' }; if (sr.results.length) { const ex = sr.results[0]; let exTags = (ex.properties.customer_tag || '').split(',').map(t => t.trim()).filter(t => t); tags.forEach(t => { if (!exTags.includes(t)) exTags.push(t); }); props.customer_tag = exTags.join(','); await hubspotClient.crm.contacts.basicApi.update(ex.id, { properties: props }); results.updated++; } else { props.customer_tag = tags.join(','); await hubspotClient.crm.contacts.basicApi.create({ properties: props }); results.created++; } } catch (e) { results.skipped++; } } res.json(results); } catch (e) { res.status(500).json({ error: e.message }); } });

// Scheduled campaign processor
setInterval(async () => { if (!pool || !hubspotClient || !sgInitialized) return; try { const result = await pool.query("SELECT * FROM campaigns WHERE status = 'scheduled' AND scheduled_time <= NOW()"); for (const campaign of result.rows) { const contactIds = campaign.contact_ids ? campaign.contact_ids.split(',').filter(x => x) : []; if (!contactIds.length) continue; const contacts = await hubspotClient.crm.contacts.batchApi.read({ inputs: contactIds.map(id => ({ id })), properties: ['firstname', 'lastname', 'email'] }); const recipients = []; const surveyBaseUrl = campaign.survey_id ? getAppUrl() + '/survey/' + campaign.survey_id : null; for (const contact of contacts.results) { const email = contact.properties.email, firstName = contact.properties.firstname || '', lastName = contact.properties.lastname || ''; const recipient = { contactId: contact.id, name: (firstName + ' ' + lastName).trim(), email, sent: false, delivered: false, opened: false, clicked: false, bounced: false, error: null }; if (!email) { recipient.error = 'No email'; recipients.push(recipient); continue; } let emailBody = campaign.body.replace(/\[First Name\]/gi, firstName); if (surveyBaseUrl) { const surveyLink = surveyBaseUrl + '?email=' + encodeURIComponent(email) + '&fname=' + encodeURIComponent(firstName) + '&lname=' + encodeURIComponent(lastName) + '&campaign_id=' + campaign.id; emailBody = emailBody.replace(/\{\{survey_link\}\}/gi, surveyLink); emailBody = emailBody.replace(/\[Survey Link\]/gi, '<a href="' + surveyLink + '" style="display:inline-block;padding:12px 24px;background:#e94560;color:white;text-decoration:none;border-radius:6px;font-weight:500">Take the Survey</a>'); } const wrappedBody = wrapEmailHTML(emailBody); const finalBody = injectTracking(wrappedBody, campaign.tracking_id); try { await sgMail.send({ to: email, from: process.env.FROM_EMAIL || 'noreply@example.com', subject: campaign.subject, html: finalBody, trackingSettings: { clickTracking: { enable: true }, openTracking: { enable: true } }, customArgs: { campaignId: campaign.id, contactId: contact.id } }); recipient.sent = true; updateHubSpotContact(contact.id); } catch (e) { recipient.error = e.message; } recipients.push(recipient); } await pool.query('UPDATE campaigns SET status = $1, sent_at = NOW(), recipients = $2 WHERE id = $3', ['sent', JSON.stringify(recipients), campaign.id]); } } catch (e) { console.error('Scheduler error:', e.message); } }, 60000);
setInterval(checkForReplies, 300000);
setInterval(checkForLeads, 300000); // Check for new leads every 5 minutes
setInterval(processLeadSequences, 60000); // Process due nurture emails every minute

const PORT = process.env.PORT || 3000;
initDatabase().then(() => { app.listen(PORT, '0.0.0.0', () => { console.log('Server running on port ' + PORT); console.log('Database: ' + (pool ? 'connected' : 'not configured')); console.log('HubSpot: ' + (hubspotClient ? 'configured' : 'not configured')); console.log('SendGrid: ' + (sgInitialized ? 'configured' : 'not configured')); console.log('Anthropic: ' + (anthropic ? 'configured' : 'not configured')); console.log('Google OAuth: ' + (oauth2Client ? 'configured' : 'not configured')); }); }).catch(e => { console.error('Failed to init database:', e.message); process.exit(1); });
