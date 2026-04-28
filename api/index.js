import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

export const runtime = 'nodejs';

export const config = {
  api: { bodyParser: false },
  supportsResponseStreaming: true,
  maxDuration: 60,
};

const TARGET_BASE = (process.env.TARGET_DOMAIN || "").replace(/\/$/, "");

const STRIP_HEADERS = new Set([
  "host", "connection", "keep-alive",
  "proxy-authenticate", "proxy-authorization", "te", "trailer",
  "transfer-encoding", "upgrade", "forwarded", "x-forwarded-host",
  "x-forwarded-proto", "x-forwarded-port",
]);

export default async function handler(req, res) {
  if (!TARGET_BASE) {
    res.statusCode = 500;
    return res.end("Misconfigured: TARGET_DOMAIN is not set");
  }

  try {
    const targetUrl = TARGET_BASE + req.url;

    const headers = {};
    let clientIp = null;
    
    // بهینه‌سازی: حلقه با for-of به جای for-in
    const headerKeys = Object.keys(req.headers);
    for (const key of headerKeys) {
      const k = key.toLowerCase();
      if (STRIP_HEADERS.has(k)) continue;
      if (k.startsWith("x-vercel-")) continue;
      
      const v = req.headers[key];
      if (k === "x-real-ip") { 
        clientIp = v; 
        continue; 
      }
      if (k === "x-forwarded-for") { 
        if (!clientIp) clientIp = v; 
        continue; 
      }
      headers[k] = Array.isArray(v) ? v.join(", ") : v;
    }
    if (clientIp) headers["x-forwarded-for"] = clientIp;

    const method = req.method;
    const hasBody = method !== "GET" && method !== "HEAD";

    const fetchOpts = { 
      method, 
      headers, 
      redirect: "manual" 
    };
    
    if (hasBody) {
      fetchOpts.body = Readable.toWeb(req);
      fetchOpts.duplex = "half";
    }

    const upstream = await fetch(targetUrl, fetchOpts);

    res.statusCode = upstream.status;
    
    // بهینه‌سازی: فقط هدرهای ضروری رو کپی کن
    for (const [k, v] of upstream.headers) {
      if (k !== "transfer-encoding" && k !== "content-length") {
        try { 
          res.setHeader(k, v); 
        } catch (_) {}
      }
    }

    if (upstream.body) {
      await pipeline(Readable.fromWeb(upstream.body), res);
    } else {
      res.end();
    }
  } catch (_) {
    if (!res.headersSent) {
      res.statusCode = 502;
      res.end("Bad Gateway: Tunnel Failed");
    }
  }
}