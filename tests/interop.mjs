// npm install --prefix /tmp/easy-sdk-js-peer easyjssdk@2.0.4
// EASYSDK_JS_DIR=/tmp/easy-sdk-js-peer WK_WS_URL=... WK_UID=bob WK_TOKEN=... node tests/interop.mjs
import { createRequire } from 'node:module';
import { resolve } from 'node:path';
const require = createRequire(resolve(process.env.EASYSDK_JS_DIR, 'package.json'));
const { WKIM, WKIMEvent, WKIMChannelType } = require('easyjssdk');
const im = WKIM.init(process.env.WK_WS_URL, { uid: process.env.WK_UID, token: process.env.WK_TOKEN, deviceFlag: 1 }, { singleton: false });
let replies = 0;
const deadline = setTimeout(() => { im.destroy(); process.exit(1); }, 30000);
im.on(WKIMEvent.Message, async (message) => {
  try {
    await im.send(message.fromUid, WKIMChannelType.Person, message.payload);
    replies += 1;
    if (replies === 2) console.info('PASS: JS peer replied twice');
  } catch { console.error('JS peer send failed'); process.exitCode = 1; im.destroy(); clearTimeout(deadline); }
});
process.on('SIGTERM', () => { im.destroy(); clearTimeout(deadline); });
try { await im.connect(); console.info('READY'); }
catch { console.error('JS peer connect failed'); im.destroy(); clearTimeout(deadline); process.exitCode = 1; }
