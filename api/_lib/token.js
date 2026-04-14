const crypto = require("crypto");

function b64urlEncode(buf) {
  return Buffer.from(buf).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function b64urlDecode(str) {
  const pad = "=".repeat((4 - (str.length % 4)) % 4);
  const s = (str + pad).replace(/-/g, "+").replace(/_/g, "/");
  return Buffer.from(s, "base64");
}

function mintAccessToken({ secret, ttlSeconds }) {
  const now = Math.floor(Date.now() / 1000);
  const payload = { iat: now, exp: now + Number(ttlSeconds) };
  const payloadJson = JSON.stringify(payload, Object.keys(payload).sort());
  const payloadB64 = b64urlEncode(Buffer.from(payloadJson, "utf8"));
  const sig = crypto.createHmac("sha256", String(secret)).update(payloadB64).digest();
  const sigB64 = b64urlEncode(sig);
  return `${payloadB64}.${sigB64}`;
}

function verifyAccessToken({ token, secret }) {
  if (!token || typeof token !== "string" || !token.includes(".")) {
    const err = new Error("Invalid token format");
    err.statusCode = 401;
    throw err;
  }
  const [payloadB64, sigB64] = token.split(".", 2);
  const expectedSig = crypto.createHmac("sha256", String(secret)).update(payloadB64).digest();
  const actualSig = b64urlDecode(sigB64);
  if (actualSig.length !== expectedSig.length || !crypto.timingSafeEqual(actualSig, expectedSig)) {
    const err = new Error("Invalid token signature");
    err.statusCode = 401;
    throw err;
  }
  const payload = JSON.parse(b64urlDecode(payloadB64).toString("utf8"));
  const exp = Number(payload?.exp ?? 0);
  if (Math.floor(Date.now() / 1000) >= exp) {
    const err = new Error("Token expired");
    err.statusCode = 401;
    throw err;
  }
  return payload;
}

module.exports = { mintAccessToken, verifyAccessToken };

