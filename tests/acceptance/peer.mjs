import { WKIM, WKIMEvent, WKIMChannelType } from 'easyjssdk';
const im = WKIM.init(process.env.WK_WS_URL, {
  uid: process.env.WK_UID, token: process.env.WK_TOKEN, deviceFlag: 1,
}, { singleton: false });
let replies = 0;
let finished = false;
let failure = null;
function finish(code) {
  if (finished) return;
  finished = true;
  im.destroy(); clearTimeout(deadline);
  console.info(JSON.stringify({ peer: 'easyjssdk@2.0.4', replies, failure, status: code === 0 ? 'pass' : 'fail' }));
  process.exitCode = code;
}
const deadline = setTimeout(() => finish(1), (Number(process.env.WK_TEST_SECONDS) + 30) * 1000);
im.on(WKIMEvent.Message, async (message) => {
  try { await im.send(message.fromUid, WKIMChannelType.Person, message.payload); replies++; }
  catch (error) { failure = { kind: "send", code: typeof error?.code === "number" ? error.code : null }; finish(1); }
});
im.on(WKIMEvent.Disconnect, (event) => { if (!finished) { failure = { kind: 'disconnect', code: typeof event?.reasonCode === 'number' ? event.reasonCode : (typeof event?.code === 'number' ? event.code : null) }; finish(1); } });
process.on('SIGTERM', () => finish(0));
try { await im.connect(); console.info('READY'); }
catch { finish(1); }
