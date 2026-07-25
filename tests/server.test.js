const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

process.env.STRIPE_SECRET_KEY = '';
process.env.PAYPAL_CLIENT_ID = '';
process.env.PAYPAL_CLIENT_SECRET = '';
process.env.PORT = '0';

const { app, buildBwmApiHeaders } = require('../index.js');
const usersPath = path.join(__dirname, '..', 'users.json');
const originalUsers = fs.existsSync(usersPath) ? fs.readFileSync(usersPath, 'utf8') : null;

async function startServer() {
  const server = app.listen(0);
  await new Promise((resolve, reject) => {
    server.once('listening', resolve);
    server.once('error', reject);
  });
  return server;
}

async function stopServer(server) {
  await new Promise((resolve, reject) => {
    server.close((err) => (err ? reject(err) : resolve()));
  });
}

test('health endpoint responds without payment secrets configured', async () => {
  const server = await startServer();

  try {
    const address = server.address();
    const response = await fetch(`http://127.0.0.1:${address.port}/health`);
    const body = await response.json();

    assert.equal(response.status, 200);
    assert.equal(body.status, 'ok');
  } finally {
    await stopServer(server);
  }
});

test('config endpoint exposes current email configuration state', async () => {
  const server = await startServer();

  try {
    const address = server.address();
    const response = await fetch(`http://127.0.0.1:${address.port}/config`);
    const body = await response.json();

    const hasSmtp = Boolean(process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS);
    const hasApi = Boolean(process.env.EMAIL_API_URL && process.env.EMAIL_API_KEY);
    const expectedConfigured = hasSmtp || hasApi;

    assert.equal(response.status, 200);
    assert.equal(body.emailConfigured, expectedConfigured);
    assert.equal(body.emailFrom, process.env.EMAIL_FROM || 'no-reply@nyoderaheights.com');
  } finally {
    await stopServer(server);
  }
});

test('buildBwmApiHeaders attaches the configured key to outbound requests', () => {
  process.env.BWM_API_KEY = 'test-key';
  const headers = buildBwmApiHeaders();

  assert.equal(headers['x-api-key'], 'test-key');
  assert.equal(headers.Authorization, 'Bearer test-key');
});

test('signup creates a verification flow and login works after verification', async () => {
  const server = await startServer();
  const email = 'auth-flow-test@example.com';
  const password = 'TestPassword123!';
  const backup = originalUsers;

  try {
    const signupResponse = await fetch(`http://127.0.0.1:${server.address().port}/api/auth/signup`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'Auth Flow Tester', email, password }),
    });
    const signupBody = await signupResponse.json();

    assert.equal(signupResponse.status, 200);
    assert.equal(signupBody.email, email);
    assert.equal(signupBody.verified, false);
    assert.equal(signupBody.requiresVerification, true);

    const users = JSON.parse(fs.readFileSync(usersPath, 'utf8'));
    const createdUser = users.find((user) => user.email === email);
    assert.ok(createdUser, 'created user should be persisted');
    assert.ok(createdUser.otp, 'signup should create a verification code');
    assert.ok(createdUser.otpExpires, 'signup should set otp expiry');

    const verifyResponse = await fetch(`http://127.0.0.1:${server.address().port}/api/auth/verify-otp`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, otp: createdUser.otp }),
    });
    const verifyBody = await verifyResponse.json();

    assert.equal(verifyResponse.status, 200);
    assert.equal(verifyBody.success, true);
    assert.equal(verifyBody.verified, true);

    const loginResponse = await fetch(`http://127.0.0.1:${server.address().port}/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password }),
    });
    const loginBody = await loginResponse.json();

    assert.equal(loginResponse.status, 200);
    assert.equal(loginBody.verified, true);
    assert.equal(loginBody.email, email);
  } finally {
    if (backup === null) {
      fs.rmSync(usersPath, { force: true });
    } else {
      fs.writeFileSync(usersPath, backup, 'utf8');
    }
    await stopServer(server);
  }
});
