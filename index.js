// A Cloudflare Worker-based Proxy Gateway with WebSocket Transport
import { connect } from 'cloudflare:sockets';

// ======================================
// 1. 安全与混淆字典 (Sensitive String Obfuscation)
// ======================================
const SEC = {
	V_PRO: base64Decode('dmxlc3M='),
	C_META: base64Decode('Y2xhc2g='),
	S_BOX: base64Decode('c2luZ2JveA=='),
	V2_R: base64Decode('djJyYXk='),
	X_R: base64Decode('eHJheQ=='),
	SS: base64Decode('c3M='),
	SR: base64Decode('c3Ny'),
	V_M: base64Decode('dm1lc3M=')
};

// ======================================
// 2. 常量与默认配置 (Constants & Defaults)
// ======================================
const DEFAULT_PROXY_IPS = ['cdn.xn--b6gac.eu.org:443', 'cdn-all.xn--b6gac.eu.org:443'];
const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;

// ======================================
// 3. 入口与上下文构建 (Entry & Context)
// ======================================
export default {
	async fetch(request, env, _ctx) {
		try {
			const appCtx = buildContext(request, env);

			if (appCtx.isInvalidUser) {
				throw new Error('UUID configuration is invalid');
			}

			// 路由分发 (Routing Dispatch)
			if (request.headers.get('Upgrade') !== 'websocket') {
				return await handleHttpRouter(request, appCtx);
			}

			return await handleWebSocketRouter(request, appCtx);
		} catch (err) {
			return new Response(err.toString(), {
				status: 500,
				headers: { "Content-Type": "text/plain; charset=utf-8" }
			});
		}
	},
};

/**
 * 动态实例化当前请求上下文 (Eliminate Global State)
 */
function buildContext(request, env) {
	const url = new URL(request.url);
	const host = request.headers.get('Host') || url.hostname;
	const userAgent = request.headers.get('User-Agent')?.toLowerCase() || '';
	const { UUID, PROXYIP, SOCKS5, SOCKS5_RELAY, URL_FORWARD, ADD, SUB, CSV, DLSstr, ONLYTLS } = env;

	// 1. 用户 ID 解析
	const userIDs = (UUID?.trim() || '').replace(/[\s,]+/g, ',').split(',').filter(Boolean);
	const isInvalidUser = userIDs.some(uuid => !isValidUUID(uuid));

	// 2. 代理 IP 解析 (优先请求参数，其次环境变量，最后回退默认)
	const [proxyIp, proxyPort] = resolveProxyIpConfig(url, PROXYIP);

	// 3. SOCKS5 解析
	let socks5Config = null;
	const s5AddressStr = SOCKS5?.trim();
	if (s5AddressStr) {
		try {
			const s5Addresses = s5AddressStr.split(/[,\s]+/);
			const selectedS5 = s5Addresses[Math.floor(Math.random() * s5Addresses.length)];
			socks5Config = parseSocks5Address(selectedS5);
		} catch (e) {
			console.error('Invalid SOCKS5 config', e);
		}
	}

	// 4. 其他配置与特征
	const pathname = url.pathname.replace(/\/+$/, '') || '/';
	const matchedUserId = userIDs.find(uuid => pathname.includes(uuid)) || '';

	const hasSubscriptionParams = url.searchParams.has("cfproxylist") || url.searchParams.has("cfproxycsv") || url.searchParams.has("proxysub");
	const target = determineSubscriptionTarget(url, userAgent);

	let onlyTls = ONLYTLS ?? true;
	if (url.searchParams.has("notTls")) onlyTls = false;
	if (host.includes('pages.dev')) onlyTls = true;

	return {
		url, host, pathname, userAgent, env,
		userIDs,
		matchedUserId,
		isInvalidUser,
		proxyIp, proxyPort,
		socks5Config,
		socks5Relay: !!SOCKS5_RELAY,
		urlForward: URL_FORWARD,
		onlyTls,
		params: { ADD, SUB, CSV, DLSstr, target, hasSubscriptionParams }
	};
}

// ======================================
// 4. HTTP 路由引擎 (HTTP Control Plane)
// ======================================
async function handleHttpRouter(request, ctx) {
	const { pathname, matchedUserId } = ctx;

	if (pathname === '/') {
		if (ctx.urlForward) return proxyForward(ctx, request);
		return renderFakeDriveView(ctx); // 伪装页
	}

	if (pathname === '/cfrequest') {
		return new Response(JSON.stringify(request.cf, null, 4), {
			status: 200,
			headers: { "Content-Type": "application/json;charset=utf-8" },
		});
	}

	// 匹配到隐藏的 UUID 路径，生成订阅和配置
	if (matchedUserId && pathname === `/${matchedUserId}`) {
		return await generateSubscription(ctx);
	}

	if (ctx.urlForward) return proxyForward(ctx, request);
	return render404Nginx();
}

function proxyForward(ctx, request) {
	const targetUrl = new URL(ctx.urlForward + ctx.url.pathname + ctx.url.search);
	const headers = new Headers([...request.headers].filter(([key]) => !key.toLowerCase().startsWith('cf-')));
	headers.set('Host', targetUrl.hostname);
	return fetch(targetUrl, { method: request.method, headers, body: request.body, redirect: 'follow' });
}

// ======================================
// 5. WebSocket 协议引擎 (WebSocket Data Plane)
// ======================================
async function handleWebSocketRouter(request, ctx) {
	const webSocketPair = new WebSocketPair();
	const [client, webSocket] = Object.values(webSocketPair);
	webSocket.accept();

	let connInfo = { address: '', portLog: '', isDns: false };
	const log = (info) => console.log(`[${connInfo.address}:${connInfo.portLog}] ${info}`);
	const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';

	const wsStream = buildWsReadableStream(webSocket, earlyDataHeader, log);
	let remoteSocketObj = { value: null }; // 使用对象保持引用

	wsStream.pipeTo(new WritableStream({
		async write(chunk, controller) {
			if (connInfo.isDns) return handleDNSQuery(chunk, webSocket, null, log);

			// 如果 Socket 已经建立，透传数据
			if (remoteSocketObj.value) {
				const writer = remoteSocketObj.value.writable.getWriter();
				await writer.write(chunk);
				writer.releaseLock();
				return;
			}

			// 尚未建立，解析握手包
			const parsedReq = decodeVProtocolHeader(chunk, ctx.userIDs);
			connInfo.address = parsedReq.addressRemote;
			connInfo.portLog = `${parsedReq.portRemote}--${Math.random()} ${parsedReq.isUDP ? 'udp' : 'tcp'}`;

			if (parsedReq.hasError) throw new Error(parsedReq.message);

			if (parsedReq.isUDP) {
				if (parsedReq.portRemote === 53) {
					connInfo.isDns = true;
					return;
				}
				throw new Error('UDP proxy is only enabled for DNS (port 53)');
			}

			// V-Protocol 回包头: ["version", "附加信息长度"]
			const vResponseHeader = new Uint8Array([parsedReq.protocolVersion[0], 0]);
			const rawClientData = chunk.slice(parsedReq.rawDataIndex);

			if (connInfo.isDns) return handleDNSQuery(rawClientData, webSocket, vResponseHeader, log);

			await establishOutboundTCP(remoteSocketObj, parsedReq, rawClientData, webSocket, vResponseHeader, ctx, log);
		},
		close: () => log(`WS readable closed`),
		abort: (reason) => log(`WS readable aborted: ${reason}`),
	})).catch(err => log(`WS pipeTo error: ${err}`));

	return new Response(null, { status: 101, webSocket: client });
}

// ======================================
// 6. 出站网络层 (Outbound Network Layer)
// ======================================
async function establishOutboundTCP(remoteSocketObj, parsedReq, rawData, ws, vHeader, ctx, log) {
	const { addressType, addressRemote, portRemote } = parsedReq;

	async function doConnect(address, port, useSocks) {
		let tcpSocket;
		if (useSocks && ctx.socks5Config) {
			tcpSocket = await connectSocks5Tunnel(addressType, address, port, ctx.socks5Config, log);
		} else {
			tcpSocket = connect({ hostname: address, port: port });
		}
		if (!tcpSocket) throw new Error('Failed to connect to outbound');

		remoteSocketObj.value = tcpSocket;
		log(`connected to ${address}:${port}`);
		const writer = tcpSocket.writable.getWriter();
		await writer.write(rawData); // 写入 Client Hello
		writer.releaseLock();
		return tcpSocket;
	}

	// 错误重试闭包
	async function retryFallback() {
		let tcpSocket;
		try {
			// 严格区分 SOCKS5 和 proxyIP 的 Fallback 路由
			if (ctx.socks5Config) {
				// 如果有 SOCKS，走 SOCKS 去真实目标地址
				tcpSocket = await doConnect(addressRemote, portRemote, true);
			} else {
				// 否则 fallback 到 CF 优选 IP (proxyIp)
				tcpSocket = await doConnect(ctx.proxyIp || addressRemote, ctx.proxyPort || portRemote, false);
			}

			// 增加底层 Socket 关闭事件的兜底拦截，防止假死
			tcpSocket.closed.catch(error => {
				log(`Retry tcpSocket closed with error: ${error}`);
			}).finally(() => {
				safeCloseSocket(ws);
			});

			bindRemoteToWs(tcpSocket, ws, vHeader, null, log);
		} catch (e) {
			log(`Fallback failed: ${e.message}`);
			safeCloseSocket(ws);
		}
	}

	try {
		// 初次尝试：如果 socks5Relay 为 true，则全部强制走 SOCKS5，否则直连
		let tcpSocket = await doConnect(addressRemote, portRemote, ctx.socks5Relay);
		bindRemoteToWs(tcpSocket, ws, vHeader, retryFallback, log);
	} catch (e) {
		log(`Initial connect failed, trying fallback. Error: ${e.message}`);
		await retryFallback();
	}
}

async function bindRemoteToWs(remoteSocket, ws, vHeader, retryCallback, log) {
	let hasData = false;
	try {
		await remoteSocket.readable.pipeTo(new WritableStream({
			async write(chunk) {
				if (ws.readyState !== WS_READY_STATE_OPEN) throw new Error('WS not open');
				hasData = true;
				if (vHeader) {
					ws.send(await new Blob([vHeader, chunk]).arrayBuffer());
					vHeader = null;
				} else {
					ws.send(chunk);
				}
			},
			close: () => log(`Remote closed, incoming data: ${hasData}`)
		}));
	} catch (error) {
		log(`RemoteSocketToWS error: ${error.message}`);
		safeCloseSocket(ws);
	}

	if (!hasData && retryCallback) {
		log(`No incoming data, triggering retry`);
		await retryCallback();
	}
}

// ======================================
// 7. 协议解析引擎 (Protocol Decoding Layer)
// ======================================
function decodeVProtocolHeader(buffer, validUuids) {
	if (buffer.byteLength < 24) return { hasError: true, message: 'invalid length' };

	const dataView = new DataView(buffer);
	const version = dataView.getUint8(0);
	const slicedUuidHex = formatUuidHex(new Uint8Array(buffer.slice(1, 17)));

	const isValid = validUuids.some(u => u.trim() === slicedUuidHex);
	if (!isValid) return { hasError: true, message: 'invalid uuid auth' };

	const optLength = dataView.getUint8(17);
	const command = dataView.getUint8(18 + optLength); // 1: TCP, 2: UDP
	if (command !== 1 && command !== 2) return { hasError: true, message: `command ${command} is not supported, command 01-tcp,02-udp,03-mux` };

	const portIndex = 18 + optLength + 1;
	const portRemote = dataView.getUint16(portIndex);
	const addressType = dataView.getUint8(portIndex + 2);
	let addrValue, addrLen, addrIndex;

	switch (addressType) {
		case 1: // IPv4
			addrLen = 4;
			addrIndex = portIndex + 3;
			addrValue = new Uint8Array(buffer.slice(addrIndex, addrIndex + addrLen)).join('.');
			break;
		case 2: // Domain
			addrLen = dataView.getUint8(portIndex + 3);
			addrIndex = portIndex + 4;
			addrValue = new TextDecoder().decode(buffer.slice(addrIndex, addrIndex + addrLen));
			break;
		case 3: // IPv6
			addrLen = 16;
			addrIndex = portIndex + 3;
			addrValue = Array.from({ length: 8 }, (_, i) => dataView.getUint16(addrIndex + i * 2).toString(16)).join(':');
			break;
		default:
			return { hasError: true, message: `unknown addr type: ${addressType}` };
	}

	return {
		hasError: false,
		addressRemote: addrValue,
		addressType,
		portRemote,
		rawDataIndex: addrIndex + addrLen,
		protocolVersion: new Uint8Array([version]),
		isUDP: command === 2
	};
}

// ======================================
// 8. SOCKS5 级联与 DNS 劫持模块
// ======================================
async function connectSocks5Tunnel(addressType, addressRemote, portRemote, s5conf, log) {
	const { username, password, hostname, port } = s5conf;
	const socket = connect({ hostname, port });
	const writer = socket.writable.getWriter();
	const reader = socket.readable.getReader();

	try {
		await writer.write(new Uint8Array([5, 2, 0, 2])); // Init
		let res = (await reader.read()).value;
		if (!res || res[0] !== 0x05) throw new Error(`SOCKS5 version err: expected 5, got ${res?.[0]}`);

		// 服务端拒绝了所有我们提供的认证方法
		if (res[1] === 0xff) {
			throw new Error('SOCKS5 server: no acceptable auth methods (0xFF)');
		}

		if (res[1] === 0x02) { // Need Auth
			if (!username || !password) throw new Error('SOCKS5 auth missing');
			const enc = new TextEncoder();
			const authReq = new Uint8Array([1, username.length, ...enc.encode(username), password.length, ...enc.encode(password)]);
			await writer.write(authReq);
			res = (await reader.read()).value;
			if (res[0] !== 0x01 || res[1] !== 0x00) throw new Error('SOCKS5 auth fail');
		}
		else if (res[1] !== 0x00) {
			// 防御性编程：如果是 0x00 (免密) 以外的其他未知方法，也应当阻断
			throw new Error(`SOCKS5 unexpected auth method: ${res[1]}`);
		}

		// Connect Command
		const enc = new TextEncoder();
		let dstAddr;
		if (addressType === 1) dstAddr = new Uint8Array([1, ...addressRemote.split('.').map(Number)]);
		else if (addressType === 2) dstAddr = new Uint8Array([3, addressRemote.length, ...enc.encode(addressRemote)]);
		else dstAddr = new Uint8Array([4, ...addressRemote.split(':').flatMap(x => [parseInt(x.slice(0, 2), 16), parseInt(x.slice(2), 16)])]);

		await writer.write(new Uint8Array([5, 1, 0, ...dstAddr, portRemote >> 8, portRemote & 0xff]));
		res = (await reader.read()).value;
		if (res[1] !== 0x00) throw new Error('SOCKS5 open fail');

		log("SOCKS5 tunnel established");
		writer.releaseLock();
		reader.releaseLock();
		return socket;
	} catch (e) {
		log(`SOCKS5 Error: ${e.message}`);
		writer.releaseLock();
		reader.releaseLock();
		return null;
	}
}

async function handleDNSQuery(udpChunk, ws, vHeader, log) {
	const dnsHost = '8.8.4.4', dnsPort = 53;
	try {
		log(`connected to ${dnsHost}:${dnsPort}`);
		const tcpSocket = connect({ hostname: dnsHost, port: dnsPort });
		const writer = tcpSocket.writable.getWriter();
		await writer.write(udpChunk);
		writer.releaseLock();

		await tcpSocket.readable.pipeTo(new WritableStream({
			async write(chunk) {
				if (ws.readyState !== WS_READY_STATE_OPEN) return;
				if (vHeader) {
					ws.send(await new Blob([vHeader, chunk]).arrayBuffer());
					vHeader = null;
				} else {
					ws.send(chunk);
				}
			},
			close() {
				log(`dns server(${dnsHost}) tcp is close`);
			},
			abort(reason) {
				console.error(`dns server(${dnsHost}) tcp is abort`, reason);
			}
		}));
	} catch (e) { log(`DNS error: ${e.message}`); }
}

// ======================================
// 9. 配置与订阅生成器 (Subscription & Generator)
// ======================================
async function generateSubscription(ctx) {
	const addresses = await fetchExternalProxies(ctx);
	return formatSubscriptionResponse(addresses, ctx);
}

function determineSubscriptionTarget(url, userAgent) {
	if (url.searchParams.get('target')) return url.searchParams.get('target').toLowerCase();
	if (url.searchParams.has('sub')) return "sub";
	if (url.searchParams.has('raw')) return "raw";
	if (url.searchParams.has('clash') || userAgent.includes('clash') || userAgent.includes('meta')) return "clash";
	if (url.searchParams.has('sb') || url.searchParams.has('singbox') || url.searchParams.has('sing-box') || userAgent.includes('singbox') || userAgent.includes('sing-box')) return "singbox";
	return "";
}

async function fetchExternalProxies(ctx) {
	const { env, url, matchedUserId, host, onlyTls, params: { DLSstr, hasSubscriptionParams } } = ctx;
	const results = [];
	const dlsStr = url.searchParams.get("DLSstr") || DLSstr || 5;
	const [textListStr, csvListStr, subProviderStr] = [
		hasSubscriptionParams ? url.searchParams.get("cfproxylist") : env.ADD,
		hasSubscriptionParams ? url.searchParams.get("cfproxycsv") : env.CSV,
		hasSubscriptionParams ? url.searchParams.get("proxysub") : env.SUB
	];

	if (textListStr) {
		const urls = textListStr.trim().split(/[,\s]+/).map(l => {
			return (l.startsWith("https://") || l.startsWith("http://")) ? l : "https://" + l;
		}).join(',');
		results.push(...await parseTextProxyList(urls));
	}
	if (csvListStr) {
		results.push(...await parseCsvProxyList(csvListStr, onlyTls, dlsStr));
	}
	if (subProviderStr) {
		results.push(...await parseSubProviderLinks(subProviderStr, matchedUserId, host));
	}

	return results;
}

function formatSubscriptionResponse(addresses, ctx) {
	const { userAgent, matchedUserId, host, onlyTls, params: { target } } = ctx;
	const uniqueTags = new Map();
	const uniqueAddrs = new Map();
	const speed_reg = /\d+\.?\d*(?= ?[MK]B\/s)/i;

	for (const [scheme, addr, port, rawTag, fullLink] of addresses) {
		const key = `${scheme}:${addr}:${port}`;
		const existing = uniqueAddrs.get(key);
		const pSpeed = speed_reg.exec(rawTag);
		const eSpeed = existing && speed_reg.exec(existing[0]);
		// 如果新链接Tag中有测速数据的Tag比没有的好，数据大的更好
		if (!(!existing || !eSpeed && pSpeed || eSpeed && pSpeed && pSpeed[0] > eSpeed[0])) continue;

		const tlsQuery = onlyTls ? "&security=tls" : "";
		let link = fullLink || `${scheme}://${matchedUserId}@${addr}:${port}?type=ws${tlsQuery}&host=${host}&sni=${host}&path=${encodeURIComponent("/?ed=2048")}#${encodeURIComponent(rawTag)}`;

		// Handle duplicate tags
		let count = uniqueTags.get(rawTag) || 0;
		uniqueTags.set(rawTag, count + 1);
		if (count > 0) link += `%20${count + 1}`;

		uniqueAddrs.set(key, [rawTag, link]);
	}

	const finalLinksStr = Array.from(uniqueAddrs.values()).map(m => m[1]).join('\n');

	// const needsConverter = target && target !== 'raw' && target !== 'sub';
	const toDashboard = !target && userAgent.includes('mozilla');
	// if (needsConverter) {
	// }

	if (toDashboard) {
		return new Response(generateClientConfigHtml(finalLinksStr, matchedUserId, host, onlyTls), {
			headers: { "Content-Type": "text/html; charset=utf-8" },
		});
	}

	return new Response(target === 'raw' ? finalLinksStr : base64Encode(finalLinksStr), {
		status: 200,
		headers: { "Content-Type": "text/plain; charset=utf-8" }
	});
}

async function parseTextProxyList(urlsStr) {
	if (!urlsStr?.trim()) return [];
	const rawTextLines = await fetchUrlsToLines(urlsStr, 8000);
	const regExp = /^((?:[\w-]+\.)+[a-z-]+|\d{1,3}(?:\.\d{1,3}){3}|\[[a-f0-9:]+\])(?::(\d+))?(?:#(.+)$)?/i;

	return rawTextLines.map(line => {
		const match = regExp.exec(line);
		if (!match) return null;
		let [, host, port = '443', tag = host] = match;
		return [SEC.V_PRO, host, port, tag];
	}).filter(Boolean);
}

async function parseCsvProxyList(csvUrlStr, onlyTls, dlsStr) {
	if (!csvUrlStr?.trim()) return [];
	const [DLS = 5, MAXROW = 8] = dlsStr.split(":").map(Number);
	const results = [];
	const csvUrls = csvUrlStr.split(/[,\s]+/);

	const handleHeader = (line) => {
		const header = line.toLowerCase().split(',').map(t => t.trim());
		const cols = {
			ip: header.findIndex(s => s.startsWith('ip') || s.includes('地址')),
			port: header.findIndex(s => s.startsWith('端口') || s.startsWith('port')),
			tls: header.indexOf('tls'),
			speed: header.findLastIndex(s => s.includes("速度") || s.includes("speed")),
			city: header.findIndex(s => s.includes('城市') || s.includes('city')),
			country: header.findIndex(s => s.includes('国家') || s.includes('country')),
			idc: header.findLastIndex(s => s.includes("数据中心") || s.includes("idc")),
			speedUnit: header[cols.speed]?.includes('kb') ? 'KB' : header[cols.speed]?.includes('mb') ? 'MB' : '',
			header: header
		};
		return cols;
	};

	for (const url of csvUrls) {
		try {
			const csvData = await fetchWithTimeout(url, 12000);
			const lines = csvData.split('\n').map(l => l.trim()).filter(Boolean);
			if (!lines.length) continue;

			// Find first header line dynamically
			const headerIdx = lines.findIndex(l =>
				(l.includes('地址') || l.toLowerCase().startsWith('ip')) &&
				(l.includes('端口') || l.toLowerCase().includes('port'))
			);
			if (headerIdx === -1) continue;

			let cols = handleHeader(lines[headerIdx]);
			if (cols.ip === -1) continue;
			let header = cols.header;
			let speedUnit = cols.speedUnit;

			let addedRows = 0;

			// Process rows below header
			for (let i = headerIdx + 1; i < lines.length && (MAXROW === 0 || addedRows < MAXROW); i++) {
				const hasNewHeader = ((lines[i].includes('地址') || lines[i].toLowerCase().startsWith('ip'))
					&& (lines[i].includes('端口') || lines[i].toLowerCase().includes('port')));
				if (hasNewHeader) {
					cols = handleHeader(lines[headerIdx]);
					if (cols.ip === -1) continue;
					header = cols.header;
					speedUnit = cols.speedUnit;
					continue;
				}
				const row = lines[i].split(',').map(t => t.trim());
				if (row.length !== header.length) continue;
				if (onlyTls && row[cols.tls]?.toLowerCase() !== "true") continue;

				// Speed Check
				if (DLS > 0 && cols.speed !== -1) {
					let speed = parseFloat(row[cols.speed]);
					if (!isNaN(speed)) {
						if (!speedUnit) speedUnit = row[cols.speed].toLowerCase().includes('kb') ? 'KB' : row[cols.speed].toLowerCase().includes('mb') ? 'MB' : '';
						if (!speedUnit && speedUnit === 'KB') speed = Math.round(speed / 10) / 100;
						if (speed < DLS) continue;
					}
				}

				const port = row[cols.port] || (row[cols.tls]?.toLowerCase() === 'true' ? '443' : '80');
				const tagParts = [row[cols.city], row[cols.country], row[cols.idc]].filter(Boolean);
				const tag = tagParts.length > 0 ? tagParts.join('-') : row[cols.ip];

				results.push([SEC.V_PRO, row[cols.ip], port, tag]);
				addedRows++;
			}
		} catch (e) {
			console.error(`CSV Fetch Error for ${url}:`, e.message);
		}
	}
	return results;
}

async function parseSubProviderLinks(subUrlStr, userID, host) {
	if (!subUrlStr?.trim()) return [];
	const subUrls = subUrlStr.trim().split(/[\n,]+/);
	const results = [];

	// 向第三方订阅生成器请求前，需要将自身的CF节点的UUID和节点地址使用假数据掩盖
	const fakeUserID = crypto.randomUUID();
	const fakeHost = generateDomain();

	for (let sub of subUrls) {
		if (sub.trim().startsWith("#")) continue;

		const fetchUrl = `${sub}/sub?host=${fakeHost}&uuid=${fakeUserID}&path=${encodeURIComponent("/?ed=2048")}`;
		try {
			let data = await fetchWithTimeout(fetchUrl, 12000, 'v2rayn.xray');
			if (isBase64(data)) data = base64Decode(data);
			data = data.replace(new RegExp(fakeUserID, 'gm'), userID).replace(new RegExp(fakeHost, 'gm'), host);
			results.push(...data.split('\n').filter(Boolean));
		} catch (e) {
			console.error(`Sub Provider Fetch Error:`, e.message);
		}
	}
	return extractStandardProxyLinks(results);
}

// Resolves a list of nested API URLs into a flat array of lines
async function fetchUrlsToLines(configsStr, timeoutMs = 8000) {
	const configs = configsStr.trim().split(/[\n,]+/);
	const results = [];

	for (let line of configs) {
		line = line.trim();
		if (!line || line.startsWith('#')) continue;

		let fetchUrl = "";
		if (line.startsWith("https://") || line.startsWith("http://")) fetchUrl = line;
		else {
			results.push(line);
			continue;
		}

		try {
			const data = await fetchWithTimeout(fetchUrl, timeoutMs);
			const nestedLines = await fetchUrlsToLines(data, timeoutMs);
			results.push(...nestedLines);
		} catch (e) {
			console.error(`Fetch API list error: ${line}`);
		}
	}
	return results;
}

function extractStandardProxyLinks(linkLines) {
	const regExp = /^(\w+):\/\/(?:[^@\s]+@)?([^?#\s\/]+)[^#\s]*(?:#(.*))?$/;
	const results = [];

	for (const line of linkLines) {
		const match = regExp.exec(line.trim());
		if (!match) continue;
		let [, scheme, authHost, tagEnc] = match;
		let host, port, tag;

		try {
			if (scheme === SEC.SR) continue;
			if (scheme === SEC.V_M) {
				const parsed = JSON.parse(base64Decode(authHost));
				host = parsed.add;
				port = parsed.port;
				tag = parsed.ps || host;
			} else {
				const parts = authHost.split(":");
				port = parts.pop();
				host = parts.join(":");
				tag = tagEnc ? decodeURIComponent(tagEnc) : host;
			}
			results.push([scheme, host, port, tag, line]);
		} catch (e) {
			continue; // Skip invalid lines safely
		}
	}
	return results;
}

function generateClientConfigHtml(linksStr, uuid, host, onlyTls) {
	const randomPath = `/${Math.random().toString(36).substring(2, 15)}?ed=2048`;
	const urlTls = `?security=tls&fp=chrome&type=ws&sni=${host}&host=${host}&path=${encodeURIComponent(randomPath)}#`;
	const urlHttp = `?security=none&fp=chrome&type=ws&host=${host}&path=${encodeURIComponent(randomPath)}#`;

	const protoLinks = [
		`${SEC.V_PRO}://${uuid}@${host}:443${urlTls}HttpsNode`
	];
	if (!onlyTls) protoLinks.push(`${SEC.V_PRO}://${uuid}@${host}:80${urlHttp}HttpNode`);

	return `<!DOCTYPE html><html><head><title>Config Generator</title><style>body{background:#111;color:#eee;font-family:sans-serif;padding:2rem}pre{background:#222;padding:1rem;border-radius:8px;color:#0f0;overflow:auto}</style></head><body>
		<h2>Gateway Configuration</h2>
		<h3>Original Links</h3>
		<pre>${protoLinks.join('\n')}</pre>
		<h3>Edge links</h3>
		<pre>${linksStr.join('\n')}</pre>
	</body></html>`;
}

// ======================================
// 10. 工具集 (Utilities & Streams)
// ======================================
function buildWsReadableStream(ws, earlyDataHead, log) {
	return new ReadableStream({
		start(ctrl) {
			ws.addEventListener('message', e => ctrl.enqueue(e.data));
			ws.addEventListener('close', () => { safeCloseSocket(ws); ctrl.close(); });
			ws.addEventListener('error', e => ctrl.error(e));

			if (earlyDataHead) {
				try {
					const buf = Uint8Array.from(atob(earlyDataHead.replace(/-/g, '+').replace(/_/g, '/')), c => c.charCodeAt(0));
					if (buf.length > 0) ctrl.enqueue(buf.buffer);
				} catch (e) { ctrl.error(e); }
			}
		},
		cancel(reason) { log(`Stream canceled: ${reason}`); safeCloseSocket(ws); }
	});
}

function resolveProxyIpConfig(url, proxyEnvStr) {
	const reqIp = url.searchParams.get("proxyip") || url.searchParams.get("pyip");
	const source = reqIp || proxyEnvStr || DEFAULT_PROXY_IPS.join(',');
	const pool = source.split(/[,\s]+/).filter(a => !a.startsWith("#"));
	const target = pool[Math.floor(Math.random() * pool.length)];

	if (target.includes('[')) { // IPv6 parse
		const match = target.match(/(\[[a-f0-9:]+\])(?::(\d+))?/i);
		return match ? [match[1], match[2] || '443'] : [null, '443'];
	}
	const [ip, port = '443'] = target.split(':');
	return [ip, port];
}

function parseSocks5Address(addr) {
	const [right, left] = addr.split("@").reverse();
	let username, password;
	if (left) [username, password] = left.split(":");
	const parts = right.split(":");
	const port = Number(parts.pop());
	return { username, password, hostname: parts.join(":"), port };
}

function formatUuidHex(arr) {
	const byteToHex = Array.from({ length: 256 }, (_, i) => (i + 0x100).toString(16).slice(1));
	return [
		byteToHex[arr[0]], byteToHex[arr[1]], byteToHex[arr[2]], byteToHex[arr[3]], '-',
		byteToHex[arr[4]], byteToHex[arr[5]], '-',
		byteToHex[arr[6]], byteToHex[arr[7]], '-',
		byteToHex[arr[8]], byteToHex[arr[9]], '-',
		byteToHex[arr[10]], byteToHex[arr[11]], byteToHex[arr[12]], byteToHex[arr[13]], byteToHex[arr[14]], byteToHex[arr[15]]
	].join('').toLowerCase();
}


async function fetchWithTimeout(url, timeoutMs = 8000, userAgent = "Mozilla/5.0") {
	const fetchUrl = (url.startsWith("https://") || url.startsWith("http://")) ? url : "https://" + url;
	const signal = AbortSignal.timeout(timeoutMs);
	const response = await fetch(fetchUrl, {
		headers: { 'User-Agent': userAgent, 'Accept': '*/*' },
		signal
	});
	if (!response.ok) throw new Error(`HTTP ${response.status}`);
	return response.text();
}

function isValidUUID(uuid) {
	return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(uuid);
}

function safeCloseSocket(socket) {
	try {
		if (socket && (socket.readyState === WS_READY_STATE_OPEN || socket.readyState === WS_READY_STATE_CLOSING)) {
			socket.close();
		}
	} catch (e) { /* ignore */ }
}

function render404Nginx() {
	return new Response(`<html><head><title>404 Not Found</title></head><body><center><h1>404 Not Found</h1></center><hr><center>nginx</center></body></html>`, { status: 404, headers: { "Content-Type": "text/html; charset=utf-8" } });
}

function renderFakeDriveView(ctx) {
	const html = `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>${ctx.host} - Cloud Drive</title><style>body{font-family:sans-serif;margin:0;padding:20px;background:#f4f4f4}.container{max-width:600px;margin:auto;background:#fff;padding:2rem;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.1);text-align:center}.icon{font-size:4rem;color:#ccc}</style></head><body><div class="container"><div class="icon">☁️</div><h2>Personal Drive Server</h2><p>Server is running correctly. Authentication required for access.</p></div></body></html>`;
	return new Response(html, { headers: { "content-type": "text/html;charset=utf-8" } });
}

function generateDomain() {
	const vowels = "aeiou";
	const consonants = "bcdfghjklmnpqrstvwxyz";
	const tlds = [".com", ".net", ".org", ".io", ".ai", ".co", ".xyz", ".top", ".tech"];
	// 生成一个“辅音 + 元音”音节
	function syllable() {
		const c = consonants[Math.floor(Math.random() * consonants.length)];
		const v = vowels[Math.floor(Math.random() * vowels.length)];
		return c + v;
	}
	// 生成一个由多个音节组成的域名部分
	function word(minSyllables = 2, maxSyllables = 4) {
		const count = Math.floor(Math.random() * (maxSyllables - minSyllables + 1)) + minSyllables;
		let result = "";
		for (let i = 0; i < count; i++) {
			result += syllable();
		}
		return result;
	}
	const secondLevel = word(2, 4);
	const tld = tlds[Math.floor(Math.random() * tlds.length)];
	const isThirdLevel = Math.random() < 0.5;
	if (isThirdLevel) {
		const thirdLevel = word(1, 2);
		return `${thirdLevel}.${secondLevel}${tld}`;
	}

	return `${secondLevel}${tld}`;
}

function base64Encode(str) {
	return btoa(String.fromCharCode(...new TextEncoder().encode(str)));
}

function base64Decode(str) {
	return new TextDecoder().decode(Uint8Array.from(atob(str), c => c.charCodeAt(0)));
}

function isBase64(str) {
	if (typeof str !== 'string' || !str) return false;
	if (!/^[A-Za-z0-9+/]+={0,2}$/.test(str)) return false;
	try {
		atob(str); // 尝试解码
		return true;
	} catch {
		return false;
	}
}