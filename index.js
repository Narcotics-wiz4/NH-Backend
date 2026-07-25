const dotenv = require('dotenv');
const express = require('express');
const cors = require('cors');
const Stripe = require('stripe');
const paypal = require('@paypal/checkout-server-sdk');
const axios = require('axios');
const crypto = require('crypto');
const nodemailer = require('nodemailer');
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const mysql = require('mysql2/promise');

dotenv.config();

function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) return;
  const parsed = dotenv.parse(fs.readFileSync(filePath, 'utf8'));
  Object.entries(parsed).forEach(([key, value]) => {
    if (process.env[key] == null || process.env[key] === '') {
      process.env[key] = value;
    }
  });
}

loadEnvFile(path.join(__dirname, '.env'));
loadEnvFile(path.join(__dirname, 'deploy.env'));

const app = express();
app.set('trust proxy', 1);

const allowedOrigins = (process.env.CORS_ALLOWED_ORIGINS || `${process.env.FRONTEND_URL || ''},https://narcotics-wiz3.github.io,http://localhost:5500,http://127.0.0.1:5500,https://panel.bwmxmd.co.ke`)
  .split(',')
  .map(origin => origin.trim())
  .filter(Boolean);

function buildBwmApiHeaders() {
  const apiKey = process.env.BWM_API_KEY || process.env.XMD_API_KEY || '';
  return {
    'x-api-key': apiKey,
    Authorization: apiKey ? `Bearer ${apiKey}` : undefined,
  };
}

const corsOptions = {
  origin(origin, callback) {
    if (!origin) return callback(null, true);

    if (allowedOrigins.includes(origin) || allowedOrigins.includes('*')) {
      return callback(null, true);
    }

    const isLocalhost = /^https?:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/.test(origin);
    if (isLocalhost) return callback(null, true);

    return callback(null, false);
  },
  methods: ['GET', 'HEAD', 'PUT', 'PATCH', 'POST', 'DELETE'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: true,
  preflightContinue: false,
  optionsSuccessStatus: 204,
};

app.use(cors(corsOptions));
app.options('*', cors(corsOptions));
app.use(express.json());

const USERS_FILE = path.join(__dirname, 'users.json');
const BOOKINGS_FILE = path.join(__dirname, 'bookings.json');
const PROPERTIES_FILE = path.join(__dirname, 'properties.json');
const ROOM_SERVICES_FILE = path.join(__dirname, 'room_services.json');

// Determine DB type and create an appropriate pool (Postgres or MySQL)
const envDbType = (process.env.DATABASE_TYPE || '').toLowerCase();
const inferredType = process.env.DATABASE_URL && process.env.DATABASE_URL.startsWith('mysql') ? 'mysql' : 'pg';
const dbClientType = envDbType === 'mysql' ? 'mysql' : (envDbType === 'pg' || envDbType === 'postgres' ? 'pg' : (process.env.DATABASE_URL ? inferredType : null));
let useDb = Boolean(dbClientType);
let pool = null;
if (dbClientType === 'pg') {
  pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_SSL === 'true' ? { rejectUnauthorized: false } : undefined,
  });
} else if (dbClientType === 'mysql') {
  const mysqlConfig = {};
  if (process.env.MYSQL_HOST) {
    mysqlConfig.host = process.env.MYSQL_HOST;
    mysqlConfig.port = parseInt(process.env.MYSQL_PORT || '3306', 10);
    mysqlConfig.user = process.env.MYSQL_USER;
    mysqlConfig.password = process.env.MYSQL_PASSWORD;
    mysqlConfig.database = process.env.MYSQL_DATABASE;
  } else if (process.env.DATABASE_URL) {
    try {
      const u = new URL(process.env.DATABASE_URL.replace(/^jdbc:/, ''));
      mysqlConfig.host = u.hostname;
      mysqlConfig.port = parseInt(u.port || '3306', 10);
      mysqlConfig.user = decodeURIComponent(u.username || '');
      mysqlConfig.password = decodeURIComponent(u.password || '');
      mysqlConfig.database = (u.pathname || '').replace('/', '');
    } catch (err) {
      // ignore
    }
  } else if (process.env.JDBC_DATABASE_URL) {
    try {
      const u = new URL(process.env.JDBC_DATABASE_URL.replace(/^jdbc:/, ''));
      mysqlConfig.host = u.hostname;
      mysqlConfig.port = parseInt(u.port || '3306', 10);
      mysqlConfig.user = decodeURIComponent(u.username || '');
      mysqlConfig.password = decodeURIComponent(u.password || '');
      mysqlConfig.database = (u.pathname || '').replace('/', '');
    } catch (err) {
      // ignore
    }
  }
  if (Object.keys(mysqlConfig).length > 0) {
    pool = mysql.createPool(Object.assign({ waitForConnections: true, connectionLimit: 10 }, mysqlConfig));
  }
}

if (!pool) {
  useDb = false;
}

// Allow forcing JSON/local storage mode (useful when the panel DB is offline)
if ((process.env.FORCE_JSON_STORAGE || '').toLowerCase() === 'true') {
  console.warn('FORCE_JSON_STORAGE=true — forcing local JSON file storage and disabling DB mode');
  useDb = false;
  pool = null;
}

function disableDbMode(reason) {
  if (!useDb) return;
  console.warn('Disabling database mode due to error:', reason);
  useDb = false;
  pool = null;
}

async function dbQuery(text, params = []) {
  if (!useDb || !pool) throw new Error('Database not configured');
  try {
    if (dbClientType === 'pg') {
      return await pool.query(text, params);
    }

    // Convert Postgres $n placeholders to MySQL ? placeholders
    const sql = text.replace(/\$(\d+)/g, '?');
    // Convert JS objects to JSON strings for JSON columns
    const safeParams = params.map(p => (p && typeof p === 'object') ? JSON.stringify(p) : p);
    const [rows] = await pool.execute(sql, safeParams);
    return { rows, rowCount: Array.isArray(rows) ? rows.length : 0 };
  } catch (err) {
    disableDbMode(err.message || err);
    throw err;
  }
}

function loadJsonData(filePath) {
  try {
    if (!fs.existsSync(filePath)) return [];
    const content = fs.readFileSync(filePath, 'utf8');
    return content ? JSON.parse(content) : [];
  } catch (err) {
    console.error(`Unable to read ${filePath}:`, err);
    return [];
  }
}

async function initStorage() {
  if (!useDb) {
    console.warn('Database not configured. Using local JSON files for storage.');
    return;
  }

  if (dbClientType === 'pg') {
    await dbQuery(`CREATE TABLE IF NOT EXISTS users (
      email TEXT PRIMARY KEY,
      payload JSONB NOT NULL
    )`);
    await dbQuery(`CREATE TABLE IF NOT EXISTS properties (
      id TEXT PRIMARY KEY,
      payload JSONB NOT NULL
    )`);
    await dbQuery(`CREATE TABLE IF NOT EXISTS bookings (
      id TEXT PRIMARY KEY,
      user_email TEXT,
      payload JSONB NOT NULL
    )`);
    await dbQuery(`CREATE TABLE IF NOT EXISTS room_services (
      id TEXT PRIMARY KEY,
      booking_id TEXT,
      user_email TEXT,
      payload JSONB NOT NULL
    )`);
  } else if (dbClientType === 'mysql') {
    await dbQuery(`CREATE TABLE IF NOT EXISTS users (
      email VARCHAR(255) PRIMARY KEY,
      payload JSON NOT NULL
    )`);
    await dbQuery(`CREATE TABLE IF NOT EXISTS properties (
      id VARCHAR(255) PRIMARY KEY,
      payload JSON NOT NULL
    )`);
    await dbQuery(`CREATE TABLE IF NOT EXISTS bookings (
      id VARCHAR(255) PRIMARY KEY,
      user_email VARCHAR(255),
      payload JSON NOT NULL
    )`);
    await dbQuery(`CREATE TABLE IF NOT EXISTS room_services (
      id VARCHAR(255) PRIMARY KEY,
      booking_id VARCHAR(255),
      user_email VARCHAR(255),
      payload JSON NOT NULL
    )`);
  }

  // Seed data if empty
  const usersRows = await dbQuery(dbClientType === 'pg' ? 'SELECT 1 FROM users LIMIT 1' : 'SELECT 1 FROM users LIMIT 1');
  if ((usersRows.rowCount || (Array.isArray(usersRows.rows) && usersRows.rows.length)) === 0) {
    const seedUsers = loadJsonData(USERS_FILE);
    for (const user of seedUsers) {
      await upsertRow('users', 'email', user.email, user);
    }
  }

  const propertiesRows = await dbQuery('SELECT 1 FROM properties LIMIT 1');
  if ((propertiesRows.rowCount || (Array.isArray(propertiesRows.rows) && propertiesRows.rows.length)) === 0) {
    const seedProperties = loadJsonData(PROPERTIES_FILE);
    for (const property of seedProperties) {
      await upsertRow('properties', 'id', String(property.id), property);
    }
  }

  const bookingsRows = await dbQuery('SELECT 1 FROM bookings LIMIT 1');
  if ((bookingsRows.rowCount || (Array.isArray(bookingsRows.rows) && bookingsRows.rows.length)) === 0) {
    const seedBookings = loadJsonData(BOOKINGS_FILE);
    for (const booking of seedBookings) {
      if (dbClientType === 'pg') {
        await dbQuery('INSERT INTO bookings(id,user_email,payload) VALUES($1,$2,$3) ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload', [String(booking.id), booking.userEmail || null, booking]);
      } else {
        await dbQuery('INSERT INTO bookings(id,user_email,payload) VALUES($1,$2,$3) ON DUPLICATE KEY UPDATE payload = VALUES(payload)', [String(booking.id), booking.userEmail || null, booking]);
      }
    }
  }

  const servicesRows = await dbQuery('SELECT 1 FROM room_services LIMIT 1');
  if ((servicesRows.rowCount || (Array.isArray(servicesRows.rows) && servicesRows.rows.length)) === 0) {
    const seedServices = loadJsonData(ROOM_SERVICES_FILE);
    for (const service of seedServices) {
      if (dbClientType === 'pg') {
        await dbQuery('INSERT INTO room_services(id,booking_id,user_email,payload) VALUES($1,$2,$3,$4) ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload', [String(service.id), service.bookingId || null, service.userEmail || null, service]);
      } else {
        await dbQuery('INSERT INTO room_services(id,booking_id,user_email,payload) VALUES($1,$2,$3,$4) ON DUPLICATE KEY UPDATE payload = VALUES(payload)', [String(service.id), service.bookingId || null, service.userEmail || null, service]);
      }
    }
  }
}

async function upsertRow(table, idColumn, idValue, payload) {
  if (!useDb) throw new Error('Database not configured');

  if (dbClientType === 'pg') {
    return dbQuery(
      `INSERT INTO ${table}(${idColumn}, payload) VALUES($1, $2)
        ON CONFLICT (${idColumn}) DO UPDATE SET payload = EXCLUDED.payload`,
      [idValue, payload]
    );
  }

  // MySQL
  return dbQuery(
    `INSERT INTO ${table}(${idColumn}, payload) VALUES($1, $2)
      ON DUPLICATE KEY UPDATE payload = VALUES(payload)`,
    [idValue, payload]
  );
}

async function readUsersFromFile() {
  if (!useDb) return loadJsonData(USERS_FILE);
  let res;
  if (dbClientType === 'pg') {
    res = await dbQuery("SELECT payload FROM users ORDER BY payload->>'email'");
    const rows = res.rows || res;
    return rows.map(r => r.payload);
  }
  // MySQL
  res = await dbQuery("SELECT payload FROM users ORDER BY JSON_UNQUOTE(JSON_EXTRACT(payload, '$.email'))");
  const rows = res.rows || res;
  return rows.map(r => {
    try { return typeof r.payload === 'string' ? JSON.parse(r.payload) : r.payload; } catch (e) { return r.payload; }
  });
}

async function saveUsersToFile(users) {
  if (!useDb) {
    try {
      fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2), 'utf8');
      return;
    } catch (err) {
      console.error('Unable to save users file:', err);
      throw err;
    }
  }

  for (const user of users) {
    await upsertRow('users', 'email', user.email, user);
  }
}

async function readBookingsFromFile() {
  if (!useDb) return loadJsonData(BOOKINGS_FILE);
  if (dbClientType === 'pg') {
    const { rows } = await dbQuery("SELECT payload FROM bookings ORDER BY payload->>'id'");
    return rows.map(row => row.payload);
  }
  const res = await dbQuery("SELECT payload FROM bookings ORDER BY JSON_UNQUOTE(JSON_EXTRACT(payload, '$.id'))");
  const rows = res.rows || res;
  return rows.map(r => { try { return typeof r.payload === 'string' ? JSON.parse(r.payload) : r.payload; } catch (e) { return r.payload; } });
}

async function readPropertiesFromFile() {
  if (!useDb) return loadJsonData(PROPERTIES_FILE);
  if (dbClientType === 'pg') {
    const { rows } = await dbQuery("SELECT payload FROM properties ORDER BY payload->>'id'");
    return rows.map(row => row.payload);
  }
  const res = await dbQuery("SELECT payload FROM properties ORDER BY JSON_UNQUOTE(JSON_EXTRACT(payload, '$.id'))");
  const rows = res.rows || res;
  return rows.map(r => { try { return typeof r.payload === 'string' ? JSON.parse(r.payload) : r.payload; } catch (e) { return r.payload; } });
}

async function savePropertiesToFile(properties) {
  if (!useDb) {
    try {
      fs.writeFileSync(PROPERTIES_FILE, JSON.stringify(properties, null, 2), 'utf8');
      return;
    } catch (err) {
      console.error('Unable to save properties file:', err);
      throw err;
    }
  }

  for (const property of properties) {
    await upsertRow('properties', 'id', String(property.id), property);
  }
}

async function saveBookingsToFile(bookings) {
  if (!useDb) {
    try {
      fs.writeFileSync(BOOKINGS_FILE, JSON.stringify(bookings, null, 2), 'utf8');
      return;
    } catch (err) {
      console.error('Unable to save bookings file:', err);
      throw err;
    }
  }

  for (const booking of bookings) {
    if (dbClientType === 'pg') {
      await dbQuery('INSERT INTO bookings(id,user_email,payload) VALUES($1,$2,$3) ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload', [String(booking.id), booking.userEmail || null, booking]);
    } else {
      await dbQuery('INSERT INTO bookings(id,user_email,payload) VALUES($1,$2,$3) ON DUPLICATE KEY UPDATE payload = VALUES(payload)', [String(booking.id), booking.userEmail || null, booking]);
    }
  }
}

async function readRoomServicesFromFile() {
  if (!useDb) return loadJsonData(ROOM_SERVICES_FILE);
  if (dbClientType === 'pg') {
    const { rows } = await dbQuery("SELECT payload FROM room_services ORDER BY payload->>'id'");
    return rows.map(row => row.payload);
  }
  const res = await dbQuery("SELECT payload FROM room_services ORDER BY JSON_UNQUOTE(JSON_EXTRACT(payload, '$.id'))");
  const rows = res.rows || res;
  return rows.map(r => { try { return typeof r.payload === 'string' ? JSON.parse(r.payload) : r.payload; } catch (e) { return r.payload; } });
}

async function saveRoomServicesToFile(services) {
  if (!useDb) {
    try {
      fs.writeFileSync(ROOM_SERVICES_FILE, JSON.stringify(services, null, 2), 'utf8');
      return;
    } catch (err) {
      console.error('Unable to save room services file:', err);
      throw err;
    }
  }

  for (const service of services) {
    if (dbClientType === 'pg') {
      await dbQuery('INSERT INTO room_services(id,booking_id,user_email,payload) VALUES($1,$2,$3,$4) ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload', [String(service.id), service.bookingId || null, service.userEmail || null, service]);
    } else {
      await dbQuery('INSERT INTO room_services(id,booking_id,user_email,payload) VALUES($1,$2,$3,$4) ON DUPLICATE KEY UPDATE payload = VALUES(payload)', [String(service.id), service.bookingId || null, service.userEmail || null, service]);
    }
  }
}

async function ensureDefaultAdminUser() {
  const users = await readUsersFromFile();
  if (!users.find(u => u.email === 'admin@nh.test')) {
    users.push({
      name: 'Admin User',
      email: 'admin@nh.test',
      password: 'Admin123!',
      role: 'admin',
      verified: true
    });
    await saveUsersToFile(users);
  }
}

const stripe = process.env.STRIPE_SECRET_KEY ? new Stripe(process.env.STRIPE_SECRET_KEY) : null;

function requireStripe() {
  if (!stripe) {
    throw new Error('Stripe is not configured');
  }
  return stripe;
}

// PayPal client helper
function paypalClient() {
  const clientId = process.env.PAYPAL_CLIENT_ID;
  const clientSecret = process.env.PAYPAL_CLIENT_SECRET;
  if (!clientId || !clientSecret) throw new Error('PayPal credentials not configured');
  const environment = (process.env.PAYPAL_MODE === 'live')
    ? new paypal.core.LiveEnvironment(clientId, clientSecret)
    : new paypal.core.SandboxEnvironment(clientId, clientSecret);
  return new paypal.core.PayPalHttpClient(environment);
}

// M-Pesa Safaricom Daraja API Setup
const DARAJA_AUTH_URL = 'https://sandbox.safaricom.co.ke/oauth/v1/generate?grant_type=client_credentials';
const DARAJA_STK_URL = 'https://sandbox.safaricom.co.ke/mpesa/stkpush/v1/processrequest';
const DARAJA_CALLBACK_URL = 'https://sandbox.safaricom.co.ke/mpesa/c2b/v1/simulate';

async function getMailTransporter() {
  if (process.env.EMAIL_PROVIDER === 'unione' && process.env.EMAIL_API_KEY) {
    const unioneUrl = process.env.EMAIL_API_URL || 'https://api.unione.example/v1/messages';
    return {
      transporter: {
        sendMail: async (mailOptions) => {
          const payload = {
            sender: mailOptions.from,
            recipients: mailOptions.to,
            subject: mailOptions.subject,
            content: [
              { type: 'text/plain', value: mailOptions.text },
              { type: 'text/html', value: mailOptions.html }
            ]
          };
          const resp = await axios.post(unioneUrl, payload, {
            headers: {
              Authorization: `Bearer ${process.env.EMAIL_API_KEY}`,
              'Content-Type': 'application/json'
            },
            timeout: 10000,
          });
          return { apiResponse: resp.data, messageId: resp.data?.id || resp.headers['x-message-id'] };
        }
      },
      mode: 'unione',
    };
  }

  if (process.env.EMAIL_API_URL && process.env.EMAIL_API_KEY) {
    return {
      transporter: {
        sendMail: async (mailOptions) => {
          const payload = {
            from: mailOptions.from,
            to: mailOptions.to,
            subject: mailOptions.subject,
            text: mailOptions.text,
            html: mailOptions.html,
          };
          const resp = await axios.post(process.env.EMAIL_API_URL, payload, {
            headers: {
              Authorization: `Bearer ${process.env.EMAIL_API_KEY}`,
              'Content-Type': 'application/json',
            },
            timeout: 10000,
          });
          return { apiResponse: resp.data, messageId: resp.data?.id || resp.headers['x-message-id'] };
        }
      },
      mode: 'api',
    };
  }

  const smtpConfigured = process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS;
  if (smtpConfigured) {
    return {
      transporter: nodemailer.createTransport({
        host: process.env.SMTP_HOST,
        port: parseInt(process.env.SMTP_PORT || '587', 10),
        secure: process.env.SMTP_SECURE === 'true',
        requireTLS: true,
        auth: {
          user: process.env.SMTP_USER,
          pass: process.env.SMTP_PASS,
        },
        tls: {
          rejectUnauthorized: process.env.SMTP_REJECT_UNAUTHORIZED !== 'false',
        },
      }),
      mode: 'smtp',
    };
  }

  console.warn('SMTP credentials not configured. Using Ethereal test account for password reset emails.');
  const testAccount = await nodemailer.createTestAccount();
  return {
    transporter: nodemailer.createTransport({
      host: testAccount.smtp.host,
      port: testAccount.smtp.port,
      secure: testAccount.smtp.secure,
      auth: {
        user: testAccount.user,
        pass: testAccount.pass,
      },
    }),
    mode: 'ethereal',
  };
}

async function sendMailWithLogging(mailOptions, label = 'email') {
  const { transporter, mode } = await getMailTransporter();
  const from = mailOptions.from || process.env.EMAIL_FROM || 'no-reply@nyoderaheights.com';
  const envelopeFrom = process.env.EMAIL_ENVELOPE_FROM || from;
  const options = Object.assign({
    from,
    envelope: Object.assign({ from: envelopeFrom, to: mailOptions.to }, mailOptions.envelope || {})
  }, mailOptions);

  const info = await transporter.sendMail(options);
  console.log(`[Mail] Sent ${label} to ${mailOptions.to} via ${mode} | response=${info.response}`);
  const previewUrl = nodemailer.getTestMessageUrl(info);
  if (previewUrl) console.log(`[Mail] Preview URL: ${previewUrl}`);
  return { info, mode, previewUrl };
}

async function sendResetEmail({ email, name, tempPassword }) {
  const subject = process.env.EMAIL_SUBJECT || 'Nyodera Heights Password Reset';
  const text = `Hello ${name || 'Nyodera Heights user'},\n\n` +
    `Your temporary password is: ${tempPassword}\n\n` +
    'Use this password to log in and update your password immediately.\n\n' +
    'If you did not request this reset, please ignore this message.';
  const html = `<p>Hello ${name || 'Nyodera Heights user'},</p>` +
    `<p>Your temporary password is: <strong>${tempPassword}</strong></p>` +
    `<p>Use this password to log in and update your password immediately.</p>` +
    `<p>If you did not request this reset, please ignore this email.</p>`;

  const { info, mode, previewUrl } = await sendMailWithLogging({
    to: email,
    subject,
    text,
    html
  }, 'password reset');

  return { previewUrl, mode, info };
}

async function sendOtpEmail({ email, name, otp }) {
  const subject = process.env.EMAIL_SUBJECT_OTP || 'Nyodera Heights Email Verification';
  const text = `Hello ${name || ''},\n\nYour verification code is: ${otp}\n\nEnter this code in the Nyodera Heights sign-up page to verify your email. The code expires in 10 minutes.`;
  const html = `<p>Hello ${name || ''},</p><p>Your verification code is: <strong>${otp}</strong></p><p>The code expires in 10 minutes.</p>`;

  const { info, mode, previewUrl } = await sendMailWithLogging({
    to: email,
    subject,
    text,
    html
  }, 'otp verification');

  return { previewUrl, mode, info };
}

async function sendBookingConfirmationEmail({ email, name, booking }) {
  try {
    console.log(`[Booking Email] Sending confirmation to ${email} for booking ${booking.id}`);
    const subject = 'Booking Confirmation - Nyodera Heights';
    
    const checkInDate = new Date(booking.checkInDate).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    const checkOutDate = new Date(booking.checkOutDate).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    
    const text = `Hello ${name || 'Guest'},\n\nYour booking at Nyodera Heights has been confirmed!\n\nBooking Details:\n` +
      `- Booking ID: ${booking.id}\n` +
      `- Property: ${booking.propertyName || booking.property || 'Property'}\n` +
      `- Check-in: ${checkInDate}\n` +
      `- Check-out: ${checkOutDate}\n` +
      `- Total Amount: $${booking.paymentAmount || '0'}\n` +
      `- Status: ${booking.status}\n\n` +
      `Thank you for choosing Nyodera Heights!\n\nIf you have any questions, please contact us.`;
    
    const html = `<p>Hello ${name || 'Guest'},</p>` +
      `<p>Your booking at Nyodera Heights has been confirmed!</p>` +
      `<h3>Booking Details:</h3>` +
      `<ul>` +
      `<li><strong>Booking ID:</strong> ${booking.id}</li>` +
      `<li><strong>Property:</strong> ${booking.propertyName || booking.property || 'Property'}</li>` +
      `<li><strong>Check-in:</strong> ${checkInDate}</li>` +
      `<li><strong>Check-out:</strong> ${checkOutDate}</li>` +
      `<li><strong>Total Amount:</strong> $${booking.paymentAmount || '0'}</li>` +
      `<li><strong>Status:</strong> ${booking.status}</li>` +
      `</ul>` +
      `<p>Thank you for choosing Nyodera Heights!</p>` +
      `<p>If you have any questions, please contact us.</p>`;

    const { info, mode, previewUrl } = await sendMailWithLogging({
      to: email,
      subject,
      text,
      html
    }, 'booking confirmation');

    return { previewUrl, mode, info };
  } catch (err) {
    console.error('[Booking Email] ✗ Error:', err.message);
    return { error: err.message, mode: 'failed' };
  }
}

async function sendBookingExtensionEmail({ email, name, booking }) {
  try {
    console.log(`[Booking Email] Sending extension notice to ${email} for booking ${booking.id}`);
    const subject = 'Your stay has been extended - Nyodera Heights';

    const newCheckOutDate = new Date(booking.checkOut).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    const extensionNights = booking.extensionNights || 0;
    const extensionAmount = booking.extensionPaidAmount || 0;

    const text = `Hello ${name || 'Guest'},\n\nYour stay at Nyodera Heights has been successfully extended.\n\nExtension Details:\n` +
      `- Booking ID: ${booking.id}\n` +
      `- Property: ${booking.propertyName || booking.property || 'Property'}\n` +
      `- New check-out date: ${newCheckOutDate}\n` +
      `- Additional nights: ${extensionNights}\n` +
      `- Additional amount: $${extensionAmount}\n\n` +
      `Thank you for staying with us. If you have any questions, please contact us.`;

    const html = `<p>Hello ${name || 'Guest'},</p>` +
      `<p>Your stay at Nyodera Heights has been successfully extended.</p>` +
      `<h3>Extension Details:</h3>` +
      `<ul>` +
      `<li><strong>Booking ID:</strong> ${booking.id}</li>` +
      `<li><strong>Property:</strong> ${booking.propertyName || booking.property || 'Property'}</li>` +
      `<li><strong>New check-out date:</strong> ${newCheckOutDate}</li>` +
      `<li><strong>Additional nights:</strong> ${extensionNights}</li>` +
      `<li><strong>Additional amount:</strong> $${extensionAmount}</li>` +
      `</ul>` +
      `<p>Thank you for staying with us. If you have any questions, please contact us.</p>`;

    const { info, mode, previewUrl } = await sendMailWithLogging({
      to: email,
      subject,
      text,
      html
    }, 'booking extension');

    return { previewUrl, mode, info };
  } catch (err) {
    console.error('[Booking Email] ✗ Extension email error:', err.message);
    return { error: err.message, mode: 'failed' };
  }
}

async function sendBookingCancellationRequestEmail({ email, name, booking }) {
  try {
    console.log(`[Booking Email] Sending cancellation request notice to ${email} for booking ${booking.id}`);
    const subject = 'Cancellation request received - Nyodera Heights';

    const text = `Hello ${name || 'Guest'},\n\nWe have received your cancellation request for booking ${booking.id} at Nyodera Heights. Our team will review the request and notify you once the cancellation is approved or denied.\n\nCurrent booking status: ${booking.status}\n\nThank you for your patience.`;

    const html = `<p>Hello ${name || 'Guest'},</p>` +
      `<p>We have received your cancellation request for booking <strong>${booking.id}</strong> at Nyodera Heights.</p>` +
      `<p>Your request is now being reviewed. We will notify you once the cancellation is approved or denied.</p>` +
      `<p><strong>Current booking status:</strong> ${booking.status}</p>` +
      `<p>Thank you for your patience.</p>`;

    const { info, mode, previewUrl } = await sendMailWithLogging({
      to: email,
      subject,
      text,
      html
    }, 'cancellation request');

    return { previewUrl, mode, info };
  } catch (err) {
    console.error('[Booking Email] ✗ Cancellation request email error:', err.message);
    return { error: err.message, mode: 'failed' };
  }
}

function sendEmailInBackground(sendFn) {
  Promise.resolve().then(async () => {
    try {
      await sendFn();
    } catch (err) {
      console.error('Background email send failed:', err);
    }
  });
}

let mpesaAccessToken = null;
let mpesaTokenExpiry = 0;

// Get M-Pesa Access Token
async function getMpesaAccessToken() {
  try {
    if (mpesaAccessToken && Date.now() < mpesaTokenExpiry) {
      return mpesaAccessToken;
    }

    const auth = Buffer.from(
      `${process.env.MPESA_CONSUMER_KEY}:${process.env.MPESA_CONSUMER_SECRET}`
    ).toString('base64');

    const response = await axios.get(DARAJA_AUTH_URL, {
      headers: { Authorization: `Basic ${auth}` }
    });

    mpesaAccessToken = response.data.access_token;
    mpesaTokenExpiry = Date.now() + (response.data.expires_in * 1000);
    return mpesaAccessToken;
  } catch (err) {
    console.error('M-Pesa token error:', err.response?.data || err.message);
    throw err;
  }
}

// Generate M-Pesa password
function generateMpesaPassword() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  const timestamp = `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
  const passkey = process.env.MPESA_PASSKEY || '';
  const shortcode = process.env.MPESA_SHORTCODE || '';
  const data = `${shortcode}${passkey}${timestamp}`;
  const password = Buffer.from(data).toString('base64');
  return { password, timestamp };
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', message: 'Nyodera Heights API backend is running' });
});

// Root endpoint for service discovery
app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    message: 'Nyodera Heights API backend',
    endpoints: [
      { path: '/', method: 'GET' },
      { path: '/health', method: 'GET' },
      { path: '/config', method: 'GET' },
      { path: '/api/users', method: 'GET' },
      { path: '/api/properties', method: 'GET' },
      { path: '/api/bookings', method: 'GET' }
    ]
  });
});

app.post('/create-checkout-session', async (req, res) => {
  try {
    const { amount, currency = 'usd' } = req.body;
    if (!amount) return res.status(400).json({ error: 'Missing amount' });

    const stripeClient = requireStripe();
    const frontendOrigin = req.headers.origin || process.env.FRONTEND_URL || process.env.PUBLIC_URL || 'http://localhost:3000';
    const session = await stripeClient.checkout.sessions.create({
      payment_method_types: ['card'],
      mode: 'payment',
      line_items: [{ price_data: { currency, product_data: { name: 'Nyodera Heights Payment' }, unit_amount: Math.round(amount * 100) }, quantity: 1 }],
      success_url: `${frontendOrigin}/payments.html?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${frontendOrigin}/payments.html?canceled=true`
    });

    res.json({ id: session.id });
  } catch (err) {
    console.error(err);
    if (err.message === 'Stripe is not configured') {
      return res.status(503).json({ error: 'Stripe is not configured' });
    }
    res.status(500).json({ error: 'Stripe error' });
  }
});

app.post('/create-paypal-order', async (req, res) => {
  try {
    const { amount, currency = 'USD' } = req.body;
    if (!amount) return res.status(400).json({ error: 'Missing amount' });

    const request = new paypal.orders.OrdersCreateRequest();
    request.prefer('return=representation');
    request.requestBody({
      intent: 'CAPTURE',
      purchase_units: [{ amount: { currency_code: currency, value: String(amount.toFixed ? amount.toFixed(2) : amount) } }]
    });

    const client = paypalClient();
    const order = await client.execute(request);
    res.json({ id: order.result.id });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'PayPal error' });
  }
});

app.get('/checkout-session/:id', async (req, res) => {
  try {
    const stripeClient = requireStripe();
    const session = await stripeClient.checkout.sessions.retrieve(req.params.id, { expand: ['payment_intent'] });
    res.json(session);
  } catch (err) {
    console.error(err);
    if (err.message === 'Stripe is not configured') {
      return res.status(503).json({ error: 'Stripe is not configured' });
    }
    res.status(500).json({ error: 'Unable to retrieve Stripe session' });
  }
});

// Public config endpoint to return non-secret keys to the client
app.get('/config', async (req, res) => {
  try {
    const smtpConfigured = Boolean(process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS);
    const apiConfigured = Boolean(process.env.EMAIL_API_URL && process.env.EMAIL_API_KEY);
    const provider = process.env.EMAIL_PROVIDER === 'unione' && process.env.EMAIL_API_KEY ? 'unione' : (apiConfigured ? 'api' : (smtpConfigured ? 'smtp' : 'ethereal'));
    const { mode } = await getMailTransporter();
    res.json({
      stripePublishableKey: process.env.STRIPE_PUBLISHABLE_KEY || null,
      paypalClientId: process.env.PAYPAL_CLIENT_ID || null,
      mpesaEnabled: true,
      emailConfigured: smtpConfigured || apiConfigured,
      emailMode: mode,
      emailFrom: process.env.EMAIL_FROM || 'no-reply@nyoderaheights.com',
      smtpConfigured,
      apiConfigured,
      provider,
      emailFallbackEnabled: mode === 'ethereal',
    });
  } catch (err) {
    console.error('config error', err);
    res.status(500).json({ error: 'Unable to load config' });
  }
});

app.get('/api/users', async (req, res) => {
  try {
    const users = await readUsersFromFile();
    res.json(users.map(({ name, email, role, verified }) => ({ name, email, role, verified })));
  } catch (err) {
    console.error('GET /api/users error', err);
    res.status(500).json({ error: 'Unable to load users' });
  }
});

// Properties endpoints
app.get('/api/properties', async (req, res) => {
  try {
    const properties = await readPropertiesFromFile();
    res.json(properties || []);
  } catch (err) {
    console.error('GET /api/properties error', err);
    res.status(500).json({ error: 'Unable to load properties' });
  }
});

app.patch('/api/properties/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const { rate_per_month } = req.body;
    if (rate_per_month == null) return res.status(400).json({ error: 'Missing rate_per_month' });

    const properties = await readPropertiesFromFile();
    const idx = properties.findIndex(p => String(p.id) === String(id));
    if (idx === -1) return res.status(404).json({ error: 'Property not found' });

    properties[idx].rate_per_month = Number(rate_per_month);
    await savePropertiesToFile(properties);
    res.json({ success: true, property: properties[idx] });
  } catch (err) {
    console.error('PATCH /api/properties/:id error', err);
    res.status(500).json({ error: 'Unable to update property' });
  }
});

// Bookings endpoints
app.get('/api/bookings', async (req, res) => {
  try {
    const bookings = await readBookingsFromFile();
    res.json(bookings || []);
  } catch (err) {
    console.error('GET /api/bookings error', err);
    res.status(500).json({ error: 'Unable to load bookings' });
  }
});

app.post('/api/bookings', async (req, res) => {
  try {
    const booking = req.body;
    if (!booking || !booking.id || !booking.userEmail) {
      return res.status(400).json({ error: 'Missing booking data' });
    }

    const bookings = await readBookingsFromFile();
    const isNewBooking = !bookings.find(b => String(b.id) === String(booking.id));
    
    const existing = bookings.find(b => String(b.id) === String(booking.id));
    if (existing) {
      Object.assign(existing, booking);
    } else {
      bookings.push(booking);
    }

    await saveBookingsToFile(bookings);
    console.log('Booking saved:', booking.id, 'userEmail:', booking.userEmail, 'isNewBooking:', isNewBooking);

    if (isNewBooking) {
      const users = await readUsersFromFile();
      const user = users.find(u => u.email === booking.userEmail);
      const userName = user?.name || booking.userName || 'Guest';
      const emailResult = await sendBookingConfirmationEmail({
        email: booking.userEmail,
        name: userName,
        booking
      });
      console.log('Booking confirmation email result:', emailResult);
    } else {
      const previousExtensionAmount = existing.extensionPaidAmount || 0;
      if (booking.extensionPaidAmount && booking.extensionPaidAmount !== previousExtensionAmount) {
        const users = await readUsersFromFile();
        const user = users.find(u => u.email === booking.userEmail);
        const userName = user?.name || booking.userName || 'Guest';
        await sendBookingExtensionEmail({
          email: booking.userEmail,
          name: userName,
          booking
        });
      }
    }

    res.json({ success: true, booking });
  } catch (err) {
    console.error('create booking error', err);
    res.status(500).json({ error: 'Unable to save booking' });
  }
});

// Request cancellation
app.post('/api/bookings/cancel-request', async (req, res) => {
  try {
    const { bookingId, refundRequested, requestedBy } = req.body || {};
    if (!bookingId) return res.status(400).json({ error: 'Missing bookingId' });

    const bookings = await readBookingsFromFile();
    const booking = bookings.find(b => String(b.id) === String(bookingId));
    if (!booking) return res.status(404).json({ error: 'Booking not found' });

    booking.status = 'cancellation_pending';
    booking.cancellation = booking.cancellation || {};
    booking.cancellation.requestedAt = new Date().toISOString();
    booking.cancellation.requestedBy = requestedBy || 'unknown';
    booking.cancellation.refundRequested = Boolean(refundRequested);
    if (booking.cancellation.refundRequested && booking.paymentId && booking.paymentAmount) {
      booking.cancellation.refundAmount = Number((booking.paymentAmount * 0.5).toFixed(2));
    }

    await saveBookingsToFile(bookings);
    console.log('Cancellation requested:', booking.id, 'userEmail:', booking.userEmail, 'status:', booking.status);

    if (booking.userEmail) {
      const users = await readUsersFromFile();
      const user = users.find(u => u.email === booking.userEmail);
      sendEmailInBackground(async () => {
        const emailResult = await sendBookingCancellationRequestEmail({
          email: booking.userEmail,
          name: user?.name || booking.userName || 'Guest',
          booking
        });
        console.log('Cancellation request email result:', emailResult);
      });
    }

    res.json({ success: true, booking });
  } catch (err) {
    console.error('cancel-request error', err);
    res.status(500).json({ error: 'Unable to request cancellation' });
  }
});

// Approve cancellation
app.post('/api/bookings/:id/approve-cancellation', async (req, res) => {
  try {
    const id = req.params.id;
    const bookings = await readBookingsFromFile();
    const booking = bookings.find(b => String(b.id) === String(id));
    if (!booking) return res.status(404).json({ error: 'Booking not found' });
    if (!booking.cancellation || booking.status !== 'cancellation_pending') return res.status(400).json({ error: 'No pending cancellation' });

    let refundResult = null;
    if (booking.cancellation.refundRequested && booking.paymentId && booking.paymentAmount) {
      try {
        const refundAmount = booking.cancellation.refundAmount || Number((booking.paymentAmount * 0.5).toFixed(2));
        const refundResp = await axios.post(`${req.protocol}://${req.get('host')}/refund`, {
          provider: booking.paymentProvider || 'stripe',
          paymentId: booking.paymentId,
          amount: refundAmount,
          currency: booking.paymentCurrency || 'USD',
          captureId: booking.paymentCaptureId || undefined
        }, { timeout: 8000 });
        refundResult = refundResp.data;
        booking.refund = { amount: refundAmount, date: new Date().toISOString(), id: refundResult.refund?.id || `REF_${Date.now()}` };
      } catch (refundErr) {
        console.error('refund during approve-cancellation failed', refundErr.response?.data || refundErr.message || refundErr);
        booking.refund = { amount: booking.cancellation.refundAmount || null, date: new Date().toISOString(), id: `REF_${Date.now()}`, fallback: true };
      }
    }

    booking.cancellation.status = 'approved';
    booking.status = 'cancelled';
    await saveBookingsToFile(bookings);

    if (booking.userEmail) {
      const users = await readUsersFromFile();
      const user = users.find(u => u.email === booking.userEmail);
      sendEmailInBackground(async () => {
        const emailResult = await sendMailWithLogging({
          to: booking.userEmail,
          subject: 'Your cancellation request was approved',
          text: `Hello ${booking.userName || ''},\n\nYour cancellation for booking ${booking.id} was approved. A refund of $${booking.refund?.amount || 'N/A'} will be processed shortly.\n\nRegards, Nyodera Heights`
        }, 'cancellation approval');
        console.log('Cancellation approval email result:', emailResult);
      });
    }

    res.json({ success: true, booking, refundResult });
  } catch (err) {
    console.error('approve-cancellation error', err);
    res.status(500).json({ error: 'Unable to approve cancellation' });
  }
});

// Deny cancellation
app.post('/api/bookings/:id/deny-cancellation', async (req, res) => {
  try {
    const id = req.params.id;
    const bookings = await readBookingsFromFile();
    const booking = bookings.find(b => String(b.id) === String(id));
    if (!booking) return res.status(404).json({ error: 'Booking not found' });
    if (!booking.cancellation || booking.status !== 'cancellation_pending') return res.status(400).json({ error: 'No pending cancellation' });

    booking.cancellation.status = 'denied';
    booking.status = 'confirmed';
    await saveBookingsToFile(bookings);

    if (booking.userEmail) {
      sendEmailInBackground(async () => {
        const emailResult = await sendMailWithLogging({
          to: booking.userEmail,
          subject: 'Your cancellation request was denied',
          text: `Hello ${booking.userName || ''},\n\nYour cancellation request for booking ${booking.id} was denied by admin. Your booking remains confirmed.\n\nRegards, Nyodera Heights`
        }, 'cancellation denial');
        console.log('Cancellation denial email result:', emailResult);
      });
    }

    res.json({ success: true, booking });
  } catch (err) {
    console.error('deny-cancellation error', err);
    res.status(500).json({ error: 'Unable to deny cancellation' });
  }
});

// Room Service Endpoints
app.get('/api/room-service-categories', (req, res) => {
  const categories = [
    {
      id: 'food_beverage',
      name: 'Food & Beverage',
      items: [
        { id: 'breakfast', name: 'Breakfast', price: 15 },
        { id: 'lunch', name: 'Lunch', price: 25 },
        { id: 'dinner', name: 'Dinner', price: 35 },
        { id: 'snacks', name: 'Snacks & Drinks', price: 10 }
      ]
    },
    {
      id: 'cleaning',
      name: 'Cleaning Services',
      items: [
        { id: 'room_cleaning', name: 'Room Cleaning', price: 30 },
        { id: 'laundry', name: 'Laundry Service', price: 20 },
        { id: 'urgent_cleaning', name: 'Urgent Cleaning (Same Day)', price: 50 }
      ]
    },
    {
      id: 'concierge',
      name: 'Concierge Services',
      items: [
        { id: 'airport_transfer', name: 'Airport Transfer', price: 40 },
        { id: 'restaurant_booking', name: 'Restaurant Booking', price: 0 },
        { id: 'tour_booking', name: 'Tour Booking Assistance', price: 0 },
        { id: 'grocery', name: 'Grocery Shopping', price: 15 }
      ]
    },
    {
      id: 'maintenance',
      name: 'Maintenance & Support',
      items: [
        { id: 'maintenance_issue', name: 'Report Maintenance Issue', price: 0 },
        { id: 'wifi_support', name: 'WiFi Support', price: 0 },
        { id: 'keys', name: 'Emergency Keys', price: 25 }
      ]
    }
  ];
  res.json(categories);
});

app.post('/api/room-service/request', async (req, res) => {
  try {
    const { bookingId, userEmail, category, service, quantity = 1, specialRequests = '' } = req.body;
    
    if (!bookingId || !userEmail || !category || !service) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    const bookings = await readBookingsFromFile();
    const booking = bookings.find(b => String(b.id) === String(bookingId) && b.userEmail === userEmail);
    
    if (!booking) {
      return res.status(404).json({ error: 'Booking not found' });
    }

    if (booking.status !== 'confirmed') {
      return res.status(400).json({ error: 'Room service only available for confirmed bookings' });
    }

    const categories = [
      {
        id: 'food_beverage',
        items: [
          { id: 'breakfast', price: 15 },
          { id: 'lunch', price: 25 },
          { id: 'dinner', price: 35 },
          { id: 'snacks', price: 10 }
        ]
      },
      {
        id: 'cleaning',
        items: [
          { id: 'room_cleaning', price: 30 },
          { id: 'laundry', price: 20 },
          { id: 'urgent_cleaning', price: 50 }
        ]
      },
      {
        id: 'concierge',
        items: [
          { id: 'airport_transfer', price: 40 },
          { id: 'restaurant_booking', price: 0 },
          { id: 'tour_booking', price: 0 },
          { id: 'grocery', price: 15 }
        ]
      },
      {
        id: 'maintenance',
        items: [
          { id: 'maintenance_issue', price: 0 },
          { id: 'wifi_support', price: 0 },
          { id: 'keys', price: 25 }
        ]
      }
    ];

    let price = 0;
    for (const cat of categories) {
      if (cat.id === category) {
        const item = cat.items.find(i => i.id === service);
        if (item) price = item.price;
        break;
      }
    }

    const roomService = {
      id: 'RS_' + Date.now(),
      bookingId,
      userEmail,
      category,
      service,
      quantity,
      price: price * quantity,
      specialRequests,
      status: 'pending',
      requestedAt: new Date().toISOString(),
      completedAt: null
    };

    const services = await readRoomServicesFromFile();
    services.push(roomService);
    await saveRoomServicesToFile(services);

    res.json({ success: true, roomService });
  } catch (err) {
    console.error('room-service request error', err);
    res.status(500).json({ error: 'Unable to request room service' });
  }
});

app.get('/api/room-service/booking/:bookingId', async (req, res) => {
  try {
    const { bookingId } = req.params;
    const userEmail = req.query.userEmail;

    if (!bookingId || !userEmail) {
      return res.status(400).json({ error: 'Missing bookingId or userEmail' });
    }

    const bookings = await readBookingsFromFile();
    const booking = bookings.find(b => String(b.id) === String(bookingId) && b.userEmail === userEmail);
    
    if (!booking) {
      return res.status(404).json({ error: 'Booking not found' });
    }

    const services = await readRoomServicesFromFile();
    const bookingServices = services.filter(s => s.bookingId === bookingId);

    res.json(bookingServices);
  } catch (err) {
    console.error('room-service get error', err);
    res.status(500).json({ error: 'Unable to get room services' });
  }
});

app.patch('/api/room-service/:id/status', async (req, res) => {
  try {
    const { id } = req.params;
    const { status } = req.body;

    if (!['pending', 'in_progress', 'completed', 'cancelled'].includes(status)) {
      return res.status(400).json({ error: 'Invalid status' });
    }

    const services = await readRoomServicesFromFile();
    const service = services.find(s => s.id === id);

    if (!service) {
      return res.status(404).json({ error: 'Room service not found' });
    }

    service.status = status;
    if (status === 'completed') {
      service.completedAt = new Date().toISOString();
    }

    await saveRoomServicesToFile(services);
    res.json({ success: true, roomService: service });
  } catch (err) {
    console.error('room-service status update error', err);
    res.status(500).json({ error: 'Unable to update room service status' });
  }
});

app.post('/api/room-service/:id/cancel', async (req, res) => {
  try {
    const { id } = req.params;
    const { userEmail } = req.body;

    if (!userEmail) {
      return res.status(400).json({ error: 'Missing userEmail' });
    }

    const services = await readRoomServicesFromFile();
    const service = services.find(s => s.id === id && s.userEmail === userEmail);

    if (!service) {
      return res.status(404).json({ error: 'Room service not found' });
    }

    if (service.status === 'completed') {
      return res.status(400).json({ error: 'Cannot cancel completed service' });
    }

    service.status = 'cancelled';
    await saveRoomServicesToFile(services);

    res.json({ success: true, roomService: service });
  } catch (err) {
    console.error('room-service cancel error', err);
    res.status(500).json({ error: 'Unable to cancel room service' });
  }
});

app.post('/api/auth/signup', async (req, res) => {
  try {
    const { name, email, password } = req.body;
    if (!name || !email || !password) {
      return res.status(400).json({ error: 'Missing name, email, or password' });
    }

    const normalizedEmail = String(email).trim().toLowerCase();
    const users = await readUsersFromFile();
    const existingUser = users.find(u => u.email === normalizedEmail);
    if (existingUser) {
      return res.status(409).json({ error: 'Email already registered' });
    }

    const now = Date.now();
    const otp = ('' + Math.floor(100000 + Math.random() * 900000));
    const user = {
      name: String(name).trim(),
      email: normalizedEmail,
      password,
      role: 'user',
      verified: false,
      otp,
      otpExpires: now + (10 * 60 * 1000),
      lastOtpSentAt: now,
    };
    users.push(user);
    await saveUsersToFile(users);

    let emailSend = { mode: 'unknown' };
    try {
      emailSend = await sendOtpEmail({ email: user.email, name: user.name, otp });
    } catch (err) {
      console.error('Failed to send signup verification OTP email', err);
      emailSend = { error: err.message || 'Unknown send error' };
    }

    res.json({
      email: user.email,
      name: user.name,
      role: user.role,
      verified: false,
      requiresVerification: true,
      emailSend,
    });
  } catch (err) {
    console.error('signup error', err);
    res.status(500).json({ error: 'Unable to sign up user' });
  }
});

app.post('/api/auth/verify-otp', async (req, res) => {
  try {
    const { email, otp } = req.body || {};
    if (!email || !otp) return res.status(400).json({ error: 'Missing email or otp' });
    const normalizedEmail = String(email).trim().toLowerCase();
    const users = await readUsersFromFile();
    const user = users.find(u => u.email === normalizedEmail);
    if (!user) return res.status(404).json({ error: 'User not found' });
    if (user.verified) return res.json({ success: true, message: 'Already verified', email: user.email, name: user.name, role: user.role });
    if (!user.otp || !user.otpExpires) return res.status(400).json({ error: 'No verification code found' });
    if (Date.now() > Number(user.otpExpires)) return res.status(400).json({ error: 'OTP expired' });
    if (String(user.otp) !== String(otp).trim()) return res.status(400).json({ error: 'Invalid OTP' });

    user.verified = true;
    delete user.otp;
    delete user.otpExpires;
    await saveUsersToFile(users);

    res.json({ success: true, email: user.email, name: user.name, role: user.role, verified: true });
  } catch (err) {
    console.error('verify-otp error', err);
    res.status(500).json({ error: 'Unable to verify OTP' });
  }
});

app.post('/api/auth/resend-otp', async (req, res) => {
  try {
    const { email } = req.body || {};
    if (!email) return res.status(400).json({ error: 'Missing email' });
    const normalizedEmail = String(email).trim().toLowerCase();
    const users = await readUsersFromFile();
    const user = users.find(u => u.email === normalizedEmail);
    if (!user) return res.status(404).json({ error: 'User not found' });
    if (user.verified) return res.status(400).json({ error: 'User already verified' });
    const now = Date.now();
    const cooldown = 60 * 1000;
    if (user.lastOtpSentAt && (now - Number(user.lastOtpSentAt) < cooldown)) {
      const wait = Math.ceil((cooldown - (now - Number(user.lastOtpSentAt))) / 1000);
      return res.status(429).json({ error: 'Too many requests', waitSeconds: wait });
    }

    const otp = ('' + Math.floor(100000 + Math.random() * 900000));
    user.otp = otp;
    user.otpExpires = now + (10 * 60 * 1000);
    user.lastOtpSentAt = now;
    await saveUsersToFile(users);

    let emailSend = { mode: 'unknown' };
    try {
      emailSend = await sendOtpEmail({ email: user.email, name: user.name, otp });
    } catch (err) {
      console.error('Failed to send resend OTP email', err);
      emailSend = { error: err.message || 'Unknown send error' };
    }

    res.json({ success: true, message: 'OTP resent', emailSend });
  } catch (err) {
    console.error('resend-otp error', err);
    res.status(500).json({ error: 'Unable to resend OTP' });
  }
});

app.post('/api/auth/send-verification', async (req, res) => {
  try {
    const { email } = req.body || {};
    if (!email) return res.status(400).json({ error: 'Missing email' });
    const normalizedEmail = String(email).trim().toLowerCase();
    const users = await readUsersFromFile();
    const user = users.find(u => u.email === normalizedEmail);
    if (!user) return res.status(404).json({ error: 'User not found' });
    if (user.verified) return res.status(400).json({ error: 'User already verified' });

    const now = Date.now();
    const cooldown = 60 * 1000;
    if (user.lastOtpSentAt && (now - Number(user.lastOtpSentAt) < cooldown)) {
      const wait = Math.ceil((cooldown - (now - Number(user.lastOtpSentAt))) / 1000);
      return res.status(429).json({ error: 'Too many requests', waitSeconds: wait });
    }

    const otp = ('' + Math.floor(100000 + Math.random() * 900000));
    user.otp = otp;
    user.otpExpires = now + (10 * 60 * 1000);
    user.lastOtpSentAt = now;
    await saveUsersToFile(users);

    let emailSend = { mode: 'unknown' };
    try {
      emailSend = await sendOtpEmail({ email: user.email, name: user.name, otp });
    } catch (err) {
      console.error('Failed to send verification OTP email', err);
      emailSend = { error: err.message || 'Unknown send error' };
    }

    res.json({ success: true, message: 'Verification email sent', emailSend });
  } catch (err) {
    console.error('send-verification error', err);
    res.status(500).json({ error: 'Unable to send verification email' });
  }
});

app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password) {
      return res.status(400).json({ error: 'Missing email or password' });
    }

    const normalizedEmail = email.trim().toLowerCase();
    const users = await readUsersFromFile();
    const user = users.find(u => u.email === normalizedEmail && u.password === password);
    if (!user) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    if (!user.verified) {
      return res.status(401).json({ error: 'Email not verified', requiresVerification: true, verified: false });
    }

    res.json({ email: user.email, name: user.name, role: user.role, verified: true });
  } catch (err) {
    console.error('login error', err);
    res.status(500).json({ error: 'Unable to login' });
  }
});

app.get('/api/auth/me', async (req, res) => {
  try {
    const email = String(req.query.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'Missing email' });

    const users = await readUsersFromFile();
    const user = users.find(u => u.email === email);
    if (!user) return res.status(404).json({ error: 'User not found' });

    res.json({ email: user.email, name: user.name, role: user.role, verified: user.verified !== false });
  } catch (err) {
    console.error('auth/me error', err);
    res.status(500).json({ error: 'Unable to load user details' });
  }
});

app.patch('/api/auth/password', async (req, res) => {
  try {
    const { email, currentPassword, newPassword } = req.body;
    if (!email || !currentPassword || !newPassword) {
      return res.status(400).json({ error: 'Missing email, current password, or new password' });
    }

    const normalizedEmail = email.trim().toLowerCase();
    const users = await readUsersFromFile();
    const user = users.find(u => u.email === normalizedEmail);
    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }

    if (user.password !== currentPassword) {
      return res.status(401).json({ error: 'Current password is incorrect' });
    }

    user.password = newPassword;
    await saveUsersToFile(users);
    res.json({ success: true, message: 'Password updated' });
  } catch (err) {
    console.error('password update error', err);
    res.status(500).json({ error: 'Unable to update password' });
  }
});

app.post('/send-reset-email', async (req, res) => {
  try {
    const { email, tempPassword } = req.body;
    if (!email || !tempPassword) {
      return res.status(400).json({ error: 'Missing email or temporary password' });
    }

    const normalizedEmail = email.trim().toLowerCase();
    const users = await readUsersFromFile();
    const user = users.find(u => u.email === normalizedEmail);
    if (!user) {
      return res.status(404).json({ error: 'No user found for that email' });
    }

    user.password = tempPassword;
    await saveUsersToFile(users);

    const { previewUrl, mode } = await sendResetEmail({ email: user.email, name: user.name, tempPassword });
    res.json({ success: true, previewUrl, mode });
  } catch (err) {
    console.error('Password reset email error:', err.response?.data || err.message || err);
    res.status(500).json({ error: 'Unable to send reset email', details: err.message });
  }
});

app.post('/create-mpesa-payment', async (req, res) => {
  try {
    const { amount, phone } = req.body;
    if (!amount) return res.status(400).json({ error: 'Missing amount' });
    if (!phone) return res.status(400).json({ error: 'Missing phone number' });
    if (!/^\+?\d{8,15}$/.test(phone)) return res.status(400).json({ error: 'Invalid phone number format' });

    if (!process.env.MPESA_CONSUMER_KEY || !process.env.MPESA_CONSUMER_SECRET) {
      console.warn('M-Pesa credentials not configured - returning simulated response');
      const transactionId = `DEMO_${Date.now()}`;
      return res.json({
        success: true,
        transaction: {
          id: transactionId,
          provider: 'M-Pesa',
          amount,
          phone: `****${phone.slice(-4)}`,
          timestamp: new Date().toISOString(),
          status: 'PENDING',
          note: 'Demo mode: Check your phone for STK prompt. For production, configure M-Pesa credentials.'
        }
      });
    }

    try {
      const accessToken = await getMpesaAccessToken();
      const { password, timestamp } = generateMpesaPassword();
      let formattedPhone = phone.replace(/^0/, '254');
      if (!formattedPhone.startsWith('254')) {
        formattedPhone = formattedPhone.startsWith('+') ? formattedPhone.substring(1) : formattedPhone;
      }

      const stkPayload = {
        BusinessShortCode: process.env.MPESA_SHORTCODE,
        Password: password,
        Timestamp: timestamp,
        TransactionType: 'CustomerPayBillOnline',
        Amount: Math.round(amount),
        PartyA: formattedPhone,
        PartyB: process.env.MPESA_SHORTCODE,
        PhoneNumber: formattedPhone,
        CallBackURL: process.env.MPESA_CALLBACK_URL || 'https://example.com/callback',
        AccountReference: 'Nyodera_Heights',
        TransactionDesc: 'Property Payment'
      };

      const safePayload = { ...stkPayload, Password: '***REDACTED***' };
      console.log('STK Push payload:', JSON.stringify(safePayload));

      const response = await axios.post(DARAJA_STK_URL, stkPayload, {
        headers: { Authorization: `Bearer ${accessToken}` }
      });

      const checkoutId = response.data.CheckoutRequestID;
      res.json({
        success: true,
        transaction: {
          id: checkoutId,
          provider: 'M-Pesa',
          amount,
          phone: `****${phone.slice(-4)}`,
          timestamp: new Date().toISOString(),
          status: 'PENDING',
          note: '✓ STK push sent! Enter PIN on your phone.'
        }
      });
    } catch (apiErr) {
      console.error('STK Push error:', apiErr.response?.data || apiErr.message);
      
      if (apiErr.response?.status === 401) {
        return res.status(400).json({
          error: 'M-Pesa credentials invalid or expired',
          hint: 'Verify MPESA_CONSUMER_KEY and MPESA_CONSUMER_SECRET in .env'
        });
      }
      throw apiErr;
    }
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'M-Pesa payment error: ' + err.message });
  }
});

app.post('/mpesa-callback', (req, res) => {
  try {
    const body = req.body;
    console.log('M-Pesa Callback:', JSON.stringify(body, null, 2));

    const isSuccess = body.Body?.stkCallback?.ResultCode === 0;
    
    if (isSuccess) {
      const callbackMetadata = body.Body.stkCallback.CallbackMetadata.Item;
      const amount = callbackMetadata.find(item => item.Name === 'Amount')?.Value;
      const phone = callbackMetadata.find(item => item.Name === 'PhoneNumber')?.Value;
      const mpesaRef = callbackMetadata.find(item => item.Name === 'MpesaReceiptNumber')?.Value;

      console.log(`✓ Payment received: ${amount} from ${phone} (Ref: ${mpesaRef})`);
    } else {
      console.log(`✗ Payment failed with code: ${body.Body?.stkCallback?.ResultCode}`);
    }

    res.json({ ResultCode: 0, ResultDesc: 'Received' });
  } catch (err) {
    console.error('Callback error:', err);
    res.status(500).json({ error: 'Callback processing failed' });
  }
});

app.post('/refund', async (req, res) => {
  try {
    const { provider, paymentId, amount, currency, captureId } = req.body;
    if (!provider || !paymentId) return res.status(400).json({ error: 'Missing provider or paymentId' });

    if (provider.toLowerCase() === 'stripe') {
      if (!process.env.STRIPE_SECRET_KEY) return res.status(400).json({ error: 'Stripe not configured' });

      const stripeClient = requireStripe();
      let paymentIntentId = null;
      try {
        const session = await stripeClient.checkout.sessions.retrieve(paymentId);
        paymentIntentId = session.payment_intent || null;
      } catch (e) {
        paymentIntentId = paymentId;
      }

      const refundParams = {};
      if (paymentIntentId) refundParams.payment_intent = paymentIntentId;
      if (amount) refundParams.amount = Math.round(Number(amount) * 100);

      const refund = await stripeClient.refunds.create(refundParams);
      return res.json({ success: true, refund });
    }

    if (provider.toLowerCase() === 'paypal') {
      const cid = captureId || paymentId;
      if (!cid) return res.status(400).json({ error: 'Missing PayPal capture id' });
      try {
        const client = paypalClient();
        const request = new paypal.payments.CapturesRefundRequest(cid);
        if (amount) {
          request.requestBody({ amount: { value: Number(amount).toFixed(2), currency_code: currency || 'USD' } });
        }
        const response = await client.execute(request);
        return res.json({ success: true, refund: response.result });
      } catch (err) {
        console.error('PayPal refund error:', err);
        return res.status(500).json({ error: 'PayPal refund failed', details: err.message || err.toString() });
      }
    }

    return res.status(400).json({ error: 'Unsupported provider' });
  } catch (err) {
    console.error('Refund endpoint error:', err);
    res.status(500).json({ error: 'Refund failed', details: err.message });
  }
});

// Catch-all: 404 for undefined routes
app.use((req, res) => {
  res.status(404).json({ error: 'API endpoint not found', path: req.path, method: req.method });
});

const HOST = process.env.HOST || '0.0.0.0';
const PORT = Number(process.env.PORT || 25005);

async function startServer() {
  try {
    await initStorage();
  } catch (err) {
    console.error('Database initialization failed, falling back to local JSON storage:', err.message || err);
    console.warn('Continuing startup in JSON file mode. Set a valid MySQL or PostgreSQL configuration to enable database storage.');
  }

  try {
    await ensureDefaultAdminUser();
  } catch (err) {
    console.error('Admin user initialization failed:', err.message || err);
    console.warn('Continuing startup with local JSON storage even though admin initialization failed.');
  }

  return new Promise((resolve) => {
    const server = app.listen(PORT, HOST, () => {
      console.log(`Nyodera Heights API backend listening on http://${HOST}:${PORT}`);
      resolve(server);
    });
  });
}

if (require.main === module) {
  startServer().catch(err => {
    console.error('Failed to start server:', err);
    process.exit(1);
  });
}

module.exports = { app, startServer, buildBwmApiHeaders };
