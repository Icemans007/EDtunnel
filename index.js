// A Cloudflare Worker-based VESS Proxy with WebSocket Transport
import { connect } from 'cloudflare:sockets';

// ======================================
// Configuration
// ======================================
/**
 * User configuration and settings
 * Generate UUID: [Windows] Press "Win + R", input cmd and run: Powershell -NoExit -Command "[guid]::NewGuid()"
 */
let userID = '';

/**
 * Array of proxy server addresses with ports
 * Format: ['hostname:port', 'hostname:port']
 */
const proxyIPs = ['cdn.xn--b6gac.eu.org:443', 'cdn-all.xn--b6gac.eu.org:443'];

// Randomly select a proxy server from the pool
let proxyIpPort = proxyIPs[Math.floor(Math.random() * proxyIPs.length)];
let proxyIP = proxyIpPort.split(':')[0];
let proxyPort = proxyIpPort.split(':')[1] || '443';

// Alternative configurations:
// Single proxy IP: let proxyIP = 'cdn.xn--b6gac.eu.org';
// IPv6 example: let proxyIP = "[2a01:4f8:c2c:123f:64:5:6810:c55a]"

/**
 * SOCKS5 proxy configuration
 * Format: 'username:password@host:port' or 'host:port'
 */
let socks5Address = '';
/**
 * SOCKS5 relay mode
 * When true: All traffic is proxied through SOCKS5
 * When false: Only Cloudflare IPs use SOCKS5
 */
let socks5Relay = false;

let parsedSocks5Address = {};
let enableSocks = false;

// 是否禁止非TLS
let onlyTls = false;
const HttpPort = new Set([80, 8080, 8880, 2052, 2086, 2095, 2082]);
const HttpsPort = new Set([443, 8443, 2053, 2096, 2087, 2083]);

/**
 * Main handler for the Cloudflare Worker. Processes incoming requests and routes them appropriately.
 * @param {import("@cloudflare/workers-types").Request} request - The incoming request object
 * @param {Object} env - Environment variables containing configuration
 * @param {string} env.UUID - User ID for authentication
 * @param {string} env.PROXYIP - Proxy server IP address
 * @param {string} env.SOCKS5 - SOCKS5 proxy configuration
 * @param {string} env.SOCKS5_RELAY - SOCKS5 relay mode flag
 * @returns {Promise<Response>} Response object
 */
export default {
	/**
	 * @param {import("@cloudflare/workers-types").Request} request
	 * @param {Object} env
	 * @param {import("@cloudflare/workers-types").ExecutionContext} _ctx
	 * @returns {Promise<Response>}
	 */
	async fetch(request, env, _ctx) {
		try {
			const url = new URL(request.url);
			const host = request.headers.get('Host');
			const userAgent = request.headers.get('User-Agent')?.toLowerCase() || '';
			// @ts-ignore
			const { UUID, PROXYIP, SOCKS5, SOCKS5_RELAY } = env;

			userID = UUID?.trim().replace(/[\s,]+/g, ',') || userID;
			if (userID.split(',').some(uuid => !isValidUUID(uuid))) {
				throw new Error('uuid is not valid');
			}

			socks5Address = SOCKS5 || socks5Address;
			socks5Relay = SOCKS5_RELAY || socks5Relay;

			[proxyIP, proxyPort] = processProxyip(url, PROXYIP, host);

			if (socks5Address) {
				try {
					// Split SOCKS5 into an array of addresses
					const socks5Addresses = socks5Address.trim().split(/[,\s]+/);
					// Randomly select one SOCKS5 address
					const selectedSocks5 = socks5Addresses[Math.floor(Math.random() * socks5Addresses.length)];
					parsedSocks5Address = socks5AddressParser(selectedSocks5);
					enableSocks = true;
				} catch (err) {
					console.log(err.toString());
					enableSocks = false;
				}
			}

			if (request.headers.get('Upgrade') !== 'websocket') {

				let pathname = url.pathname.toLowerCase().trim();
				if (pathname.length > 1 && pathname.slice(-1) === '/') {
					pathname = pathname.slice(0, -1);
				}
				let userID_Path = userID.split(',').find(uuid => pathname.includes(uuid)) || "";

				switch (true) {
					case pathname === '/':
						// if (env.URL_FORWARD) return handleForward(env, request);
						// 伪装页面
						return handleDefaultPath(url, request);
					case pathname === '/cfrequest':
						return new Response(JSON.stringify(request.cf, null, 4), {
							status: 200,
							headers: { "Content-Type": "application/json;charset=utf-8" },
						});
					case pathname === '/convertersubrequest':
					case pathname === `/${userID_Path}`:
						const args = {
							userID: userID_Path,
							host,
							url,
							userAgent,
							proxyIP,
							ENV: env,
						};
						return GenSub(args);
					case pathname === `/bestip/${userID_Path}`:
						return fetch(`https://bestip.06151953.xyz/auto?host=${host}&uuid=${userID_Path}&path=/`, { headers: request.headers });
					// 以下不能正常执行，不能引用项目自身
					// return fetchUrl(`https://${host}/${userID_Path}?cfproxygener=bestip.06151953.xyz`, 0, null, userAgent);
					default:
						// if (env.URL_FORWARD) return handleForward(env, request);
						return new Response(`<html>
<head><title>${host} - Cloud Drive</title></head>
<body>
<center><h1>404 Not Found</h1></center>
<hr><center>nginx</center>
</body>
</html>
<!-- a padding to disable MSIE and Chrome friendly error page -->
<!-- a padding to disable MSIE and Chrome friendly error page -->
<!-- a padding to disable MSIE and Chrome friendly error page -->
<!-- a padding to disable MSIE and Chrome friendly error page -->
<!-- a padding to disable MSIE and Chrome friendly error page -->`, {
							status: 404,
							headers: { "Content-Type": "text/html; charset=utf-8" }
						});
				}

			} else {
				return await ProtocolOverWSHandler(request);
			}
		} catch (err) {
			return new Response(err.toString(), {
				status: 500,
				headers: { "Content-Type": "text/plain; charset=utf-8" }
			});
		}
	},
};

/**
 * 处理 ProxyIP
 *
 * @param {*} url
 * @param {*} PROXYIP
 * @param {*} fetch
 */
function processProxyip(url, PROXYIP, fetch = false) {
	let iproxyIP, iproxyPort;
	let requestProxyip = url.searchParams.get("proxyip") || url.searchParams.get("pyip");

	if (requestProxyip || PROXYIP) {
		// Split PROXYIP into an array of proxy addresses
		// const proxyAddresses = await fetchConfig(PROXYIP, fetch);
		const proxyAddresses = (requestProxyip || PROXYIP).trim().split(/[,\s]+/).filter(addr => addr.charAt(0) !== "#");
		// Randomly select one proxy address
		const selectedProxy = proxyAddresses[Math.floor(Math.random() * proxyAddresses.length)];
		if (!selectedProxy.includes('[')) {
			[iproxyIP, iproxyPort = '443'] = selectedProxy.split(':');
		}
		else {
			[, iproxyIP, iproxyPort = '443'] = selectedProxy.match(/(\[[a-f0-9:]+\])(?::(\d+))?/i);
		}
	}
	else {
		iproxyIP = proxyIP;
		iproxyPort = proxyPort;
	}

	return [iproxyIP, iproxyPort];
}

/**
 * @param {Object} env
 * @param {Request} request - The incoming request object
 * @returns {Promise<Response>}
 */
function handleForward(env, request) {
	const url = new URL(request.url);
	const targetUrl = new URL(env.URL_FORWARD + url.pathname + url.search);
	// 复制原始头，并移除/修改敏感头
	const headers = new Headers(
		[...request.headers].filter(([key]) => !key.toLowerCase().startsWith('cf-'))
	);

	// 强制设置正确的 Host 头
	headers.set('Host', targetUrl.hostname);
	// 转发请求
	return fetch(targetUrl, {
		method: request.method,
		headers: headers,
		body: request.body,
		redirect: 'follow'
	});
}

/**
 * Handles default path requests when no specific route matches.
 * Generates and returns a cloud drive interface HTML page.
 * @param {URL} url - The URL object of the request
 * @param {Request} request - The incoming request object
 * @returns {Response} HTML response with cloud drive interface
 */
function handleDefaultPath(url, request) {
	const host = request.headers.get('Host');
	const DrivePage = `
	  <!DOCTYPE html>
	  <html lang="en">
	  <head>
		  <meta charset="UTF-8">
		  <meta name="viewport" content="width=device-width, initial-scale=1.0">
		  <title>${host} - Cloud Drive</title>
		  <style>
			  body {
				  font-family: Arial, sans-serif;
				  line-height: 1.6;
				  margin: 0;
				  padding: 20px;
				  background-color: #f4f4f4;
			  }
			  .container {
				  max-width: 800px;
				  margin: auto;
				  background: white;
				  padding: 20px;
				  border-radius: 5px;
				  box-shadow: 0 0 10px rgba(0,0,0,0.1);
			  }
			  h1 {
				  color: #333;
			  }
			  .file-list {
				  list-style-type: none;
				  padding: 0;
			  }
			  .file-list li {
				  background: #f9f9f9;
				  margin-bottom: 10px;
				  padding: 10px;
				  border-radius: 3px;
				  display: flex;
				  align-items: center;
			  }
			  .file-list li:hover {
				  background: #f0f0f0;
			  }
			  .file-icon {
				  margin-right: 10px;
				  font-size: 1.2em;
			  }
			  .file-link {
				  text-decoration: none;
				  color: #0066cc;
				  flex-grow: 1;
			  }
			  .file-link:hover {
				  text-decoration: underline;
			  }
			  .upload-area {
				  margin-top: 20px;
				  padding: 40px;
				  background: #e9e9e9;
				  border: 2px dashed #aaa;
				  border-radius: 5px;
				  text-align: center;
				  cursor: pointer;
				  transition: all 0.3s ease;
			  }
			  .upload-area:hover, .upload-area.drag-over {
				  background: #d9d9d9;
				  border-color: #666;
			  }
			  .upload-area h2 {
				  margin-top: 0;
				  color: #333;
			  }
			  #fileInput {
				  display: none;
			  }
			  .upload-icon {
				  font-size: 48px;
				  color: #666;
				  margin-bottom: 10px;
			  }
			  .upload-text {
				  font-size: 18px;
				  color: #666;
			  }
			  .upload-status {
				  margin-top: 20px;
				  font-style: italic;
				  color: #666;
			  }
			  .file-actions {
				  display: flex;
				  gap: 10px;
			  }
			  .delete-btn {
				  color: #ff4444;
				  cursor: pointer;
				  background: none;
				  border: none;
				  padding: 5px;
			  }
			  .delete-btn:hover {
				  color: #ff0000;
			  }
			  .clear-all-btn {
				  background-color: #ff4444;
				  color: white;
				  border: none;
				  padding: 10px 15px;
				  border-radius: 4px;
				  cursor: pointer;
				  margin-bottom: 20px;
			  }
			  .clear-all-btn:hover {
				  background-color: #ff0000;
			  }
		  </style>
	  </head>
	  <body>
		  <div class="container">
			  <h1>Cloud Drive</h1>
			  <p>Welcome to your personal cloud storage. Here are your uploaded files:</p>
			  <button id="clearAllBtn" class="clear-all-btn">Clear All Files</button>
			  <ul id="fileList" class="file-list">
			  </ul>
			  <div id="uploadArea" class="upload-area">
				  <div class="upload-icon">📁</div>
				  <h2>Upload a File</h2>
				  <p class="upload-text">Drag and drop a file here or click to select</p>
				  <input type="file" id="fileInput" hidden>
			  </div>
			  <div id="uploadStatus" class="upload-status"></div>
		  </div>
		  <script>
			  function loadFileList() {
				  const fileList = document.getElementById('fileList');
				  const savedFiles = JSON.parse(localStorage.getItem('uploadedFiles')) || [];
				  fileList.innerHTML = '';
				  savedFiles.forEach((file, index) => {
					  const li = document.createElement('li');
					  li.innerHTML = \`
						  <span class="file-icon">📄</span>
						  <a href="https://ipfs.io/ipfs/\${file.Url.split('/').pop()}" class="file-link" target="_blank">\${file.Name}</a>
						  <div class="file-actions">
							  <button class="delete-btn" onclick="deleteFile(\${index})">
								  <span class="file-icon">❌</span>
							  </button>
						  </div>
					  \`;
					  fileList.appendChild(li);
				  });
			  }

			  function deleteFile(index) {
				  const savedFiles = JSON.parse(localStorage.getItem('uploadedFiles')) || [];
				  savedFiles.splice(index, 1);
				  localStorage.setItem('uploadedFiles', JSON.stringify(savedFiles));
				  loadFileList();
			  }

			  document.getElementById('clearAllBtn').addEventListener('click', () => {
				  if (confirm('Are you sure you want to clear all files?')) {
					  localStorage.removeItem('uploadedFiles');
					  loadFileList();
				  }
			  });

			  loadFileList();

			  const uploadArea = document.getElementById('uploadArea');
			  const fileInput = document.getElementById('fileInput');
			  const uploadStatus = document.getElementById('uploadStatus');

			  uploadArea.addEventListener('dragover', (e) => {
				  e.preventDefault();
				  uploadArea.classList.add('drag-over');
			  });

			  uploadArea.addEventListener('dragleave', () => {
				  uploadArea.classList.remove('drag-over');
			  });

			  uploadArea.addEventListener('drop', (e) => {
				  e.preventDefault();
				  uploadArea.classList.remove('drag-over');
				  const files = e.dataTransfer.files;
				  if (files.length) {
					  handleFileUpload(files[0]);
				  }
			  });

			  uploadArea.addEventListener('click', () => {
				  fileInput.click();
			  });

			  fileInput.addEventListener('change', (e) => {
				  const file = e.target.files[0];
				  if (file) {
					  handleFileUpload(file);
				  }
			  });

			  async function handleFileUpload(file) {
				  uploadStatus.textContent = \`Uploading: \${file.name}...\`;

				  const formData = new FormData();
				  formData.append('file', file);

				  try {
					  const response = await fetch('https://app.img2ipfs.org/api/v0/add', {
						  method: 'POST',
						  body: formData,
						  headers: {
							  'Accept': 'application/json',
						  },
					  });

					  if (!response.ok) {
						  throw new Error('Upload failed');
					  }

					  const result = await response.json();
					  uploadStatus.textContent = \`File uploaded successfully! IPFS Hash: \${result.Hash}\`;

					  const savedFiles = JSON.parse(localStorage.getItem('uploadedFiles')) || [];
					  savedFiles.push(result);
					  localStorage.setItem('uploadedFiles', JSON.stringify(savedFiles));

					  loadFileList();

				  } catch (error) {
					  console.error('Error:', error);
					  uploadStatus.textContent = 'Upload failed. Please try again.';
				  }
			  }
		  </script>
	  </body>
	  </html>
	  <!-- a padding to disable MSIE and Chrome friendly error page -->
	  <!-- a padding to disable MSIE and Chrome friendly error page -->
	  <!-- a padding to disable MSIE and Chrome friendly error page -->
	  <!-- a padding to disable MSIE and Chrome friendly error page -->
	  <!-- a padding to disable MSIE and Chrome friendly error page -->
	`;

	// 返回伪装的网盘页面
	return new Response(DrivePage, {
		headers: {
			"content-type": "text/html;charset=utf-8",
		},
	});
}

/**
 * Handles protocol over WebSocket requests by creating a WebSocket pair, accepting the WebSocket connection, and processing the protocol header.
 * @param {import("@cloudflare/workers-types").Request} request - The incoming request object
 * @returns {Promise<Response>} WebSocket response
 */
async function ProtocolOverWSHandler(request) {

	/** @type {import("@cloudflare/workers-types").WebSocket[]} */
	// @ts-ignore
	const webSocketPair = new WebSocketPair();
	const [client, webSocket] = Object.values(webSocketPair);

	webSocket.accept();

	let address = '';
	let portWithRandomLog = '';
	const log = (/** @type {string} */ info, /** @type {string | undefined} */ event) => {
		console.log(`[${address}:${portWithRandomLog}] ${info}`, event || '');
	};
	const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';

	const readableWebSocketStream = MakeReadableWebSocketStream(webSocket, earlyDataHeader, log);

	/** @type {{ value: import("@cloudflare/workers-types").Socket | null}}*/
	let remoteSocketWapper = {
		value: null,
	};
	let isDns = false;

	// ws --> remote
	readableWebSocketStream.pipeTo(new WritableStream({
		// @ts-ignore
		async write(chunk, controller) {
			if (isDns) {
				return await handleDNSQuery(chunk, webSocket, null, log);
			}
			if (remoteSocketWapper.value) {
				const writer = remoteSocketWapper.value.writable.getWriter()
				await writer.write(chunk);
				writer.releaseLock();
				return;
			}

			const {
				hasError,
				message,
				addressType,
				portRemote = 443,
				addressRemote = '',
				rawDataIndex,
				ProtocolVersion = new Uint8Array([0, 0]),
				isUDP,
			} = ProcessProtocolHeader(chunk, userID);
			address = addressRemote;
			portWithRandomLog = `${portRemote}--${Math.random()} ${isUDP ? 'udp ' : 'tcp '
				} `;
			if (hasError) {
				// controller.error(message);
				throw new Error(message); // cf seems has bug, controller.error will not end stream
			}
			// Handle UDP connections for DNS (port 53) only
			if (isUDP) {
				if (portRemote === 53) {
					isDns = true;
				} else {
					throw new Error('UDP proxy is only enabled for DNS (port 53)');
				}
				return; // Early return after setting isDns or throwing error
			}
			// ["version", "附加信息长度 N"]
			const ProtocolResponseHeader = new Uint8Array([ProtocolVersion[0], 0]);
			const rawClientData = chunk.slice(rawDataIndex);

			if (isDns) {
				return handleDNSQuery(rawClientData, webSocket, ProtocolResponseHeader, log);
			}
			// @ts-ignore
			HandleTCPOutBound(remoteSocketWapper, addressType, addressRemote, portRemote, rawClientData, webSocket, ProtocolResponseHeader, log);
		},
		close() {
			log(`readableWebSocketStream is close`);
		},
		abort(reason) {
			log(`readableWebSocketStream is abort`, JSON.stringify(reason));
		},
	})).catch((err) => {
		log('readableWebSocketStream pipeTo error', err);
	});

	return new Response(null, {
		status: 101,
		// @ts-ignore
		webSocket: client,
	});
}

/**
 * Handles outbound TCP connections for the proxy.
 * Establishes connection to remote server and manages data flow.
 * @param {Socket} remoteSocket - Socket for remote connection
 * @param {number} addressType - Type of address (IPv4/IPv6)
 * @param {string} addressRemote - Remote server address
 * @param {number} portRemote - Remote server port
 * @param {Uint8Array} rawClientData - Raw data from client
 * @param {WebSocket} webSocket - WebSocket connection
 * @param {Uint8Array} ProtocolResponseHeader - Protocol response header
 * @param {Function} log - Logging function
 */
async function HandleTCPOutBound(remoteSocket, addressType, addressRemote, portRemote, rawClientData, webSocket, ProtocolResponseHeader, log) {
	async function connectAndWrite(address, port, socks = false) {
		/** @type {import("@cloudflare/workers-types").Socket} */
		let tcpSocket;
		if (socks5Relay) {
			tcpSocket = await socks5Connect(addressType, address, port, log)
		} else {
			tcpSocket = socks ? await socks5Connect(addressType, address, port, log)
				: connect({
					hostname: address,
					port: port,
				});
		}
		// @ts-ignore
		remoteSocket.value = tcpSocket;
		log(`connected to ${address}:${port}`);
		const writer = tcpSocket.writable.getWriter();
		await writer.write(rawClientData); // first write, normal is tls client hello
		writer.releaseLock();
		return tcpSocket;
	}

	// if the cf connect tcp socket have no incoming data, we retry to redirect ip
	async function retry() {
		if (enableSocks) {
			tcpSocket = await connectAndWrite(addressRemote, portRemote, true);
		} else {
			tcpSocket = await connectAndWrite(proxyIP || addressRemote, proxyPort || portRemote, false);
		}
		// no matter retry success or not, close websocket
		tcpSocket.closed.catch(error => {
			console.log('retry tcpSocket closed error', error);
		}).finally(() => {
			safeCloseWebSocket(webSocket);
		})
		RemoteSocketToWS(tcpSocket, webSocket, ProtocolResponseHeader, null, log);
	}

	let tcpSocket = await connectAndWrite(addressRemote, portRemote);

	// when remoteSocket is ready, pass to websocket
	// remote--> ws
	RemoteSocketToWS(tcpSocket, webSocket, ProtocolResponseHeader, retry, log);
}

/**
 * Creates a readable stream from WebSocket server.
 * Handles early data and WebSocket messages.
 * @param {WebSocket} webSocketServer - WebSocket server instance
 * @param {string} earlyDataHeader - Header for early data (0-RTT)
 * @param {Function} log - Logging function
 * @returns {ReadableStream} Stream of WebSocket data
 */
function MakeReadableWebSocketStream(webSocketServer, earlyDataHeader, log) {
	// @ts-ignore
	let readableStreamCancel = false;
	const stream = new ReadableStream({
		start(controller) {
			webSocketServer.addEventListener('message', (event) => {
				const message = event.data;
				controller.enqueue(message);
			});

			webSocketServer.addEventListener('close', () => {
				safeCloseWebSocket(webSocketServer);
				controller.close();
			});

			webSocketServer.addEventListener('error', (err) => {
				log('webSocketServer has error');
				controller.error(err);
			});
			const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
			if (error) {
				controller.error(error);
			} else if (earlyData) {
				controller.enqueue(earlyData);
			}
		},

		pull(_controller) {
			// if ws can stop read if stream is full, we can implement backpressure
			// https://streams.spec.whatwg.org/#example-rs-push-backpressure
		},

		cancel(reason) {
			log(`ReadableStream was canceled, due to ${reason}`)
			readableStreamCancel = true;
			safeCloseWebSocket(webSocketServer);
		}
	});

	return stream;
}

/**
 * Processes VESS protocol header.
 * Extracts and validates protocol information from buffer.
 * @param {ArrayBuffer} protocolBuffer - Buffer containing protocol header
 * @param {string} userID - User ID for validation
 * @returns {Object} Processed header information
 */
function ProcessProtocolHeader(protocolBuffer, userID) {
	if (protocolBuffer.byteLength < 24) {
		return { hasError: true, message: 'invalid data' };
	}

	const dataView = new DataView(protocolBuffer);
	const version = dataView.getUint8(0);
	const slicedBufferString = stringify(new Uint8Array(protocolBuffer.slice(1, 17)));

	const uuids = userID.includes(',') ? userID.split(",") : [userID];
	const isValidUser = (uuids.length === 1 && slicedBufferString === uuids[0].trim()) ||
		uuids.some(uuid => slicedBufferString === uuid.trim());

	console.log(`userID: ${slicedBufferString}`);

	if (!isValidUser) {
		return { hasError: true, message: 'invalid user' };
	}

	const optLength = dataView.getUint8(17);
	const command = dataView.getUint8(18 + optLength);

	if (command !== 1 && command !== 2) {
		return { hasError: true, message: `command ${command} is not supported, command 01-tcp,02-udp,03-mux` };
	}

	const portIndex = 18 + optLength + 1;
	const portRemote = dataView.getUint16(portIndex);
	const addressType = dataView.getUint8(portIndex + 2);
	let addressValue, addressLength, addressValueIndex;

	switch (addressType) {
		case 1:
			addressLength = 4;
			addressValueIndex = portIndex + 3;
			addressValue = new Uint8Array(protocolBuffer.slice(addressValueIndex, addressValueIndex + addressLength)).join('.');
			break;
		case 2:
			addressLength = dataView.getUint8(portIndex + 3);
			addressValueIndex = portIndex + 4;
			addressValue = new TextDecoder().decode(protocolBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
			break;
		case 3:
			addressLength = 16;
			addressValueIndex = portIndex + 3;
			addressValue = Array.from({ length: 8 }, (_, i) => dataView.getUint16(addressValueIndex + i * 2).toString(16)).join(':');
			break;
		default:
			return { hasError: true, message: `invalid addressType: ${addressType}` };
	}

	if (!addressValue) {
		return { hasError: true, message: `addressValue is empty, addressType is ${addressType}` };
	}

	return {
		hasError: false,
		addressRemote: addressValue,
		addressType,
		portRemote,
		rawDataIndex: addressValueIndex + addressLength,
		protocolVersion: new Uint8Array([version]),
		isUDP: command === 2
	};
}

/**
 * Converts remote socket connection to WebSocket.
 * Handles data transfer between socket and WebSocket.
 * @param {Socket} remoteSocket - Remote socket connection
 * @param {WebSocket} webSocket - WebSocket connection
 * @param {ArrayBuffer} protocolResponseHeader - Protocol response header
 * @param {Function} retry - Retry function for failed connections
 * @param {Function} log - Logging function
 */
async function RemoteSocketToWS(remoteSocket, webSocket, protocolResponseHeader, retry, log) {
	let hasIncomingData = false;

	try {
		await remoteSocket.readable.pipeTo(
			new WritableStream({
				async write(chunk) {
					if (webSocket.readyState !== WS_READY_STATE_OPEN) {
						throw new Error('WebSocket is not open');
					}

					hasIncomingData = true;

					if (protocolResponseHeader) {
						webSocket.send(await new Blob([protocolResponseHeader, chunk]).arrayBuffer());
						protocolResponseHeader = null;
					} else {
						webSocket.send(chunk);
					}
				},
				close() {
					log(`Remote connection readable closed. Had incoming data: ${hasIncomingData}`);
				},
				abort(reason) {
					console.error(`Remote connection readable aborted:`, reason);
				},
			})
		);
	} catch (error) {
		console.error(`RemoteSocketToWS error:`, error.stack || error);
		safeCloseWebSocket(webSocket);
	}

	if (!hasIncomingData && retry) {
		log(`No incoming data, retrying`);
		await retry();
	}
}

/**
 * Converts base64 string to ArrayBuffer.
 * @param {string} base64Str - Base64 encoded string
 * @returns {Object} Object containing decoded data or error
 */
function base64ToArrayBuffer(base64Str) {
	if (!base64Str) {
		return { earlyData: null, error: null };
	}
	try {
		// Convert modified Base64 for URL (RFC 4648) to standard Base64
		base64Str = base64Str.replace(/-/g, '+').replace(/_/g, '/');
		// Decode Base64 string
		const binaryStr = atob(base64Str);
		// Convert binary string to ArrayBuffer
		const buffer = new ArrayBuffer(binaryStr.length);
		const view = new Uint8Array(buffer);
		for (let i = 0; i < binaryStr.length; i++) {
			view[i] = binaryStr.charCodeAt(i);
		}
		return { earlyData: buffer, error: null };
	} catch (error) {
		return { earlyData: null, error };
	}
}

/**
 * Validates UUID format.
 * @param {string} uuid - UUID string to validate
 * @returns {boolean} True if valid UUID
 */
function isValidUUID(uuid) {
	// More precise UUID regex pattern
	const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
	return uuidRegex.test(uuid);
}

const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;

/**
 * Safely closes WebSocket connection.
 * Prevents exceptions during WebSocket closure.
 * @param {WebSocket} socket - WebSocket to close
 */
function safeCloseWebSocket(socket) {
	try {
		if (socket.readyState === WS_READY_STATE_OPEN || socket.readyState === WS_READY_STATE_CLOSING) {
			socket.close();
		}
	} catch (error) {
		console.error('safeCloseWebSocket error:', error);
	}
}

const byteToHex = Array.from({ length: 256 }, (_, i) => (i + 0x100).toString(16).slice(1));

/**
 * Converts byte array to hex string without validation.
 * @param {Uint8Array} arr - Byte array to convert
 * @param {number} offset - Starting offset
 * @returns {string} Hex string
 */
function unsafeStringify(arr, offset = 0) {
	return [
		byteToHex[arr[offset]],
		byteToHex[arr[offset + 1]],
		byteToHex[arr[offset + 2]],
		byteToHex[arr[offset + 3]],
		'-',
		byteToHex[arr[offset + 4]],
		byteToHex[arr[offset + 5]],
		'-',
		byteToHex[arr[offset + 6]],
		byteToHex[arr[offset + 7]],
		'-',
		byteToHex[arr[offset + 8]],
		byteToHex[arr[offset + 9]],
		'-',
		byteToHex[arr[offset + 10]],
		byteToHex[arr[offset + 11]],
		byteToHex[arr[offset + 12]],
		byteToHex[arr[offset + 13]],
		byteToHex[arr[offset + 14]],
		byteToHex[arr[offset + 15]]
	].join('').toLowerCase();
}

/**
 * Safely converts byte array to hex string with validation.
 * @param {Uint8Array} arr - Byte array to convert
 * @param {number} offset - Starting offset
 * @returns {string} Hex string
 */
function stringify(arr, offset = 0) {
	const uuid = unsafeStringify(arr, offset);
	if (!isValidUUID(uuid)) {
		throw new TypeError("Stringified UUID is invalid");
	}
	return uuid;
}

/**
 * Handles DNS query through UDP.
 * Processes DNS requests and forwards them.
 * @param {ArrayBuffer} udpChunk - UDP data chunk
 * @param {WebSocket} webSocket - WebSocket connection
 * @param {ArrayBuffer} protocolResponseHeader - Protocol response header
 * @param {Function} log - Logging function
 */
async function handleDNSQuery(udpChunk, webSocket, protocolResponseHeader, log) {
	// no matter which DNS server client send, we alwasy use hard code one.
	// beacsue someof DNS server is not support DNS over TCP
	try {
		const dnsServer = '8.8.4.4'; // change to 1.1.1.1 after cf fix connect own ip bug
		const dnsPort = 53;
		/** @type {ArrayBuffer | null} */
		let vessHeader = protocolResponseHeader;
		/** @type {import("@cloudflare/workers-types").Socket} */
		const tcpSocket = connect({
			hostname: dnsServer,
			port: dnsPort,
		});

		log(`connected to ${dnsServer}:${dnsPort}`);
		const writer = tcpSocket.writable.getWriter();
		await writer.write(udpChunk);
		writer.releaseLock();
		await tcpSocket.readable.pipeTo(new WritableStream({
			async write(chunk) {
				if (webSocket.readyState === WS_READY_STATE_OPEN) {
					if (vessHeader) {
						webSocket.send(await new Blob([vessHeader, chunk]).arrayBuffer());
						vessHeader = null;
					} else {
						webSocket.send(chunk);
					}
				}
			},
			close() {
				log(`dns server(${dnsServer}) tcp is close`);
			},
			abort(reason) {
				console.error(`dns server(${dnsServer}) tcp is abort`, reason);
			},
		}));
	} catch (error) {
		console.error(
			`handleDNSQuery have exception, error: ${error.message}`
		);
	}
}

/**
 * Establishes SOCKS5 proxy connection.
 * @param {number} addressType - Type of address
 * @param {string} addressRemote - Remote address
 * @param {number} portRemote - Remote port
 * @param {Function} log - Logging function
 * @returns {Promise<Socket>} Connected socket
 */
async function socks5Connect(addressType, addressRemote, portRemote, log) {
	const { username, password, hostname, port } = parsedSocks5Address;
	// Connect to the SOCKS server
	const socket = connect({
		hostname,
		port,
	});

	// Request head format (Worker -> Socks Server):
	// +----+----------+----------+
	// |VER | NMETHODS | METHODS  |
	// +----+----------+----------+
	// | 1  |    1     | 1 to 255 |
	// +----+----------+----------+

	// https://en.wikipedia.org/wiki/SOCKS#SOCKS5
	// For METHODS:
	// 0x00 NO AUTHENTICATION REQUIRED
	// 0x02 USERNAME/PASSWORD https://datatracker.ietf.org/doc/html/rfc1929
	const socksGreeting = new Uint8Array([5, 2, 0, 2]);

	const writer = socket.writable.getWriter();

	await writer.write(socksGreeting);
	log('sent socks greeting');

	const reader = socket.readable.getReader();
	const encoder = new TextEncoder();
	let res = (await reader.read()).value;
	// Response format (Socks Server -> Worker):
	// +----+--------+
	// |VER | METHOD |
	// +----+--------+
	// | 1  |   1    |
	// +----+--------+
	if (res[0] !== 0x05) {
		log(`socks server version error: ${res[0]} expected: 5`);
		return;
	}
	if (res[1] === 0xff) {
		log("no acceptable methods");
		return;
	}

	// if return 0x0502
	if (res[1] === 0x02) {
		log("socks server needs auth");
		if (!username || !password) {
			log("please provide username/password");
			return;
		}
		// +----+------+----------+------+----------+
		// |VER | ULEN |  UNAME   | PLEN |  PASSWD  |
		// +----+------+----------+------+----------+
		// | 1  |  1   | 1 to 255 |  1   | 1 to 255 |
		// +----+------+----------+------+----------+
		const authRequest = new Uint8Array([
			1,
			username.length,
			...encoder.encode(username),
			password.length,
			...encoder.encode(password)
		]);
		await writer.write(authRequest);
		res = (await reader.read()).value;
		// expected 0x0100
		if (res[0] !== 0x01 || res[1] !== 0x00) {
			log("fail to auth socks server");
			return;
		}
	}

	// Request data format (Worker -> Socks Server):
	// +----+-----+-------+------+----------+----------+
	// |VER | CMD |  RSV  | ATYP | DST.ADDR | DST.PORT |
	// +----+-----+-------+------+----------+----------+
	// | 1  |  1  | X'00' |  1   | Variable |    2     |
	// +----+-----+-------+------+----------+----------+
	// ATYP: address type of following address
	// 0x01: IPv4 address
	// 0x03: Domain name
	// 0x04: IPv6 address
	// DST.ADDR: desired destination address
	// DST.PORT: desired destination port in network octet order

	// addressType
	// 1--> ipv4  addressLength =4
	// 2--> domain name
	// 3--> ipv6  addressLength =16
	let DSTADDR;	// DSTADDR = ATYP + DST.ADDR
	switch (addressType) {
		case 1:
			DSTADDR = new Uint8Array(
				[1, ...addressRemote.split('.').map(Number)]
			);
			break;
		case 2:
			DSTADDR = new Uint8Array(
				[3, addressRemote.length, ...encoder.encode(addressRemote)]
			);
			break;
		case 3:
			DSTADDR = new Uint8Array(
				[4, ...addressRemote.split(':').flatMap(x => [parseInt(x.slice(0, 2), 16), parseInt(x.slice(2), 16)])]
			);
			break;
		default:
			log(`invalid addressType is ${addressType}`);
			return;
	}
	const socksRequest = new Uint8Array([5, 1, 0, ...DSTADDR, portRemote >> 8, portRemote & 0xff]);
	await writer.write(socksRequest);
	log('sent socks request');

	res = (await reader.read()).value;
	// Response format (Socks Server -> Worker):
	//  +----+-----+-------+------+----------+----------+
	// |VER | REP |  RSV  | ATYP | BND.ADDR | BND.PORT |
	// +----+-----+-------+------+----------+----------+
	// | 1  |  1  | X'00' |  1   | Variable |    2     |
	// +----+-----+-------+------+----------+----------+
	if (res[1] === 0x00) {
		log("socks connection opened");
	} else {
		log("fail to open socks connection");
		return;
	}
	writer.releaseLock();
	reader.releaseLock();
	return socket;
}

/**
 * Parses SOCKS5 address string.
 * @param {string} address - SOCKS5 address string
 * @returns {Object} Parsed address information
 */
function socks5AddressParser(address) {
	let [latter, former] = address.split("@").reverse();
	let username, password, hostname, port;
	if (former) {
		const formers = former.split(":");
		if (formers.length !== 2) {
			throw new Error('Invalid SOCKS address format');
		}
		[username, password] = formers;
	}
	const latters = latter.split(":");
	port = Number(latters.pop());
	if (isNaN(port)) {
		throw new Error('Invalid SOCKS address format');
	}
	hostname = latters.join(":");
	const regex = /^\[.*\]$/;
	if (hostname.includes(":") && !regex.test(hostname)) {
		throw new Error('Invalid SOCKS address format');
	}
	return {
		username,
		password,
		hostname,
		port,
	}
}

const at = atob('UUE9PQ==');
const pt = atob('ZG14bGMzTT0=');
const ed = atob('UlVSMGRXNXVaV3c9');

/**
 * Generates configuration for VESS client.
 * @param {string} userID - userID
 * @param {string} hostName - Host name for configuration
 * @returns {string} Configuration HTML
 */
function getConfig(userID, hostName) {
	const randomPath = () => '/' + Math.random().toString(36).substring(2, 15) + '?ed=2560';
	const commonUrlPartHttp = `?encryption=none&security=none&fp=chrome&type=ws&host=${hostName}&path=${encodeURIComponent(randomPath())}#`;
	const commonUrlPartHttps = `?encryption=none&security=tls&sni=${hostName}&fp=chrome&type=ws&host=${hostName}&path=%2F%3Fed%3D2560#`;

	// Prepare output string for userID
	const sublink = `https://${hostName}/${userID}`;
	const subbestip = `https://${hostName}/bestip/${userID}`;
	// HTML Head with CSS and FontAwesome library
	const htmlHead = `
  <head>
    <title>${atob(ed)}: Configuration</title>
    <meta name='viewport' content='width=device-width, initial-scale=1'>
    <style>
      body {
        font-family: 'Roboto', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        background-color: #000000;
        color: #ffffff;
        line-height: 1.6;
        padding: 20px;
        max-width: 1200px;
        margin: 0 auto;
      }
      .container {
        background-color: #111111;
        border-radius: 8px;
        box-shadow: 0 4px 6px rgba(255, 255, 255, 0.1);
        padding: 20px;
        margin-bottom: 20px;
      }
      h1, h2 {
        color: #ffffff;
      }
      .config-item {
        background-color: #222222;
        border: 1px solid #333333;
        border-radius: 4px;
        padding: 15px;
        margin-bottom: 15px;
      }
      .config-item h3 {
        margin-top: 0;
        color: #ffffff;
      }
      .btn {
        background-color: #ffffff;
        color: #000000;
        border: none;
        padding: 10px 15px;
        border-radius: 4px;
        cursor: pointer;
        transition: background-color 0.3s, color 0.3s;
      }
      .btn:hover {
        background-color: #cccccc;
      }
      .btn-group {
        margin-top: 10px;
      }
      .btn-group .btn {
        margin-right: 10px;
      }
      pre {
        background-color: #333333;
        border: 1px solid #444444;
        border-radius: 4px;
        padding: 10px;
        white-space: pre-wrap;
        word-wrap: break-word;
        color: #00ff00;
      }
      .logo {
        float: left;
        margin-right: 20px;
        margin-bottom: 20px;
		max-width: 30%;
      }
      @media (max-width: 768px) {
        .logo {
          float: none;
          display: block;
          margin: 0 auto 20px;
          max-width: 90%; /* Adjust the max-width to fit within the container */
        }
        .btn-group {
          display: flex;
          flex-direction: column;
          align-items: center;
        }
        .btn-group .btn {
          margin-bottom: 10px;
          width: 100%;
          text-align: center;
        }
      }
      .code-container {
        position: relative;
        margin-bottom: 15px;
      }
      .code-container pre {
        margin: 0;
        padding-right: 100px; /* Make space for the button */
      }
      .copy-btn {
        position: absolute;
        top: 5px;
        right: 5px;
        padding: 5px 10px;
        font-size: 0.8em;
      }
      .subscription-info {
        margin-top: 20px;
        background-color: #222222;
        border-radius: 4px;
        padding: 15px;
      }
      .subscription-info h3 {
        color: #ffffff;
        margin-top: 0;
      }
      .subscription-info ul {
        padding-left: 20px;
      }
      .subscription-info li {
        margin-bottom: 10px;
      }
    </style>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.3/css/all.min.css">
  </head>
  `;

	const header = `
    <div class="container">
      <h1>订阅配置（优选）</h1>
      <p>Welcome! This function generates configuration for the ${atob(pt)} protocol. If you found this useful, please check our GitHub project:</p>
      <p><a href="https://github.com/Icemans007/${atob(ed)}" target="_blank" style="color: #00ff00;">${atob(ed)} - https://github.com/Icemans007/${atob(ed)}</a></p>
      <div style="clear: both;"></div>
      <div class="btn-group">
        <a href="javascript: void(0)" onclick="copyToClipboard('${sublink}')" class="btn"><i class="fas fa-star"></i>自适应订阅（推荐！）</a>
        <a href="clash://install-config?url=${encodeURIComponent(sublink + "?clash")}" class="btn" target="_blank"><i class="fas fa-bolt"></i> Clash-Meta 订阅</a>
        <a href="${sublink}?clash" class="btn" target="_blank"><i class="fas fa-link"></i> Clash Link</a>
        <a href="${sublink}?singbox" class="btn" target="_blank"><i class="fas fa-link"></i> Singbox Link</a>
		<a href="${subbestip}" class="btn" target="_blank"><i class="fas fa-star"></i> Best IP Subscription</a>
      </div>
      <div class="subscription-info">
        <h3>选项说明:</h3>
        <ul>
          <li><strong>自适应订阅:</strong> 客户端自适应的链接（仅适用于支持 ${atob(pt)} 协议 的客户端）。为许多<b>不同国家/地区</b>提供最佳服务器 IP 的精选列表</li>
          <li><strong>Clash-Meta 订阅:</strong> 打开具有预配置设置的 Clash 客户端。最适合移动设备上的 Clash 用户。</li>
          <li><strong>Clash-Meta Link:</strong> 用于将 Clash 配置转换为 Clash 格式的 Web 链接。对于手动导入或故障排除很有用。</li>
          <li><strong>Singbox Link:</strong> 用于将 Singbox 的 Web 链接。对于手动导入或故障排除很有用。</li>
		  <li><strong>Best IP Subscription:</strong> Provides a curated list of optimal server IPs for many <b>different countries</b>.</li>
        </ul>
        <p>选择最适合您的客户和需求的选项。</p>
      </div>
    </div>
  `;

	const configOutput = function () {
		let vessPart = [];
		let crashPart = ["proxies:"];

		vessPart = vessPart.concat(Array.from(HttpsPort).map(port => {
			const urlPart = encodeURIComponent(`${hostName}-HTTPS-${port}`);
			return atob(pt) + '://' + userID + atob(at) + hostName + ':' + port + commonUrlPartHttps + urlPart;
		}));

		crashPart = crashPart.concat(Array.from(HttpsPort).map(port => {
			return `  - name: ${hostName}-HTTPS-${port}
    server: ${hostName}
    port: ${port}
    type: ${atob(pt)}
    uuid: ${userID}
    network: ws
    tls: true
    udp: true
    sni: ${hostName}
    client-fingerprint: chrome
    ws-opts:
    path: "/?ed=2560"
    headers:
      host: ${hostName}`;
		}));

		if (!onlyTls) {
			vessPart = vessPart.concat(Array.from(HttpPort).map(port => {
				const urlPart = encodeURIComponent(`${hostName}-HTTP-${port}`);
				return atob(pt) + '://' + userID + atob(at) + hostName + ':' + port + commonUrlPartHttp + urlPart;
			}));

			crashPart = crashPart.concat(Array.from(HttpPort).map(port => {
				return `  - name: ${hostName}-HTTP-${port}
    server: ${hostName}
    port: ${port}
    type: ${atob(pt)}
    uuid: ${userID}
    network: ws
    tls: none
    udp: true
    sni: ${hostName}
    client-fingerprint: chrome
    ws-opts:
    path: "/?ed=2560"
    headers:
      host: ${hostName}`;
			}));
		}

		let codehtml = function (partType) {
			let html = '';
			for (let code of partType) {
				html += `${code}\n`;
			}
			return html;
		};

		return `
      <div class="container config-item">
	  	<h2>代理配置</h2>
        <h4>UUID: ${userID}</h2>
        <h4>PROXYIP: ${proxyIP}:${proxyPort}</h2>
        <h3>${atob(pt)} 配置</h3>
		<div class="code-container">
		  <pre><code>${codehtml(vessPart)}</code></pre>
		  <button class="btn copy-btn" onclick='copyToClipboard("${btoa(codehtml(vessPart))}", true)'><i class="fas fa-copy"></i> 复制</button>
		</div>
        <h3>clash 配置</h3>
		<div class="code-container">
		  <pre><code>${codehtml(crashPart)}</code></pre>
		  <button class="btn copy-btn" onclick='copyToClipboard("${btoa(codehtml(crashPart))}", true)'><i class="fas fa-copy"></i> 复制</button>
		</div>
      </div>
    `;
	}

	return `
  <html>
  ${htmlHead}
  <body>
    ${header}
    ${configOutput()}
  </body>
  <script>
    function copyToClipboard(text, conver=false) {
      navigator.clipboard.writeText(conver ? atob(text) : text)
        .then(() => {
          alert("Copied to clipboard");
        })
        .catch((err) => {
          console.error("Failed to copy to clipboard:", err);
        });
    }
  </script>
  </html>`;
}

/**
 * Generates subscription content.
 * @returns {Promise<Response>} Subscription content
 */
async function GenSub({ userID, host, userAgent, url, proxyIP, ENV }) {

	let { ADD, SUB, CSV, DLSstr, SUBCONVER, ACL4SSR_CONFIG, ONLYTLS } = ENV;

	// 订阅链接转换 crash/sing-box 的服务器后端地址
	let subconverter = SUBCONVER;
	// 订阅转换配置文件
	let subConverterMode = ACL4SSR_CONFIG || "https://raw.githubusercontent.com/ACL4SSR/ACL4SSR/refs/heads/master/Clash/config/ACL4SSR_Online.ini";

	let target = "";
	if (url.searchParams.has('sub')) {
		target = "sub";
	}
	else if (url.searchParams.has('clash') || userAgent.includes('clash')) {
		target = "clash";
	}
	else if (url.searchParams.has('singbox') || url.searchParams.has('sing-box')
		|| userAgent.includes('singbox') || userAgent.includes('sing-box')) {
		target = "singbox";
	}

	// 是否是第三方后端订阅转换服务请求 https://${host}/convertersubrequest
	let isSubReq = url.pathname.toLowerCase().startsWith("/convertersubrequest");
	let hasProxyParams = false;
	if (url.searchParams.has("cfproxylist") || url.searchParams.has("cfproxycsv") || url.searchParams.has("cfproxysub")) {
		hasProxyParams = true;
	}

	onlyTls = ONLYTLS ?? true;
	if (url.searchParams.has("notTls")) {
		onlyTls = false;
	}
	if (host.includes('pages.dev')) {
		onlyTls = true;
	}

	if (!target && !hasProxyParams && !isSubReq && userAgent.toLowerCase().includes('mozilla')) {
		return new Response(getConfig(userID, host), {
			status: 200,
			headers: { "Content-Type": "text/html; charset=utf-8" },
		});
	}

	let fakeUserID = generateRandomUUID();
	let fakeHost = generateRandomStr(12) + [".net", ".com", ".org", ".edu", ".cn", ".jp", ".xyz", ".us"].at(Math.random() * 8 | 0);

	if (!isSubReq && (target === "clash" || target === "singbox")) {
		if (url.searchParams.has("subconverter")) {
			subconverter = url.searchParams.get("subconverter")?.trim();
		}

		if (!subconverter) {
			return new Response(`服务没有绑定后端订阅转换服务，请使用"&subconverter="传递订阅转换服务地址`, {
				status: 412,
				headers: {
					'Content-Type': 'text/html; charset=utf-8',
				}
			});
		}

		// 连接协议
		let subconverSplit = subconverter.split("://");
		if (subconverSplit.length < 2) {
			// 有时候是本地服务，所以默认是非TLS
			subconverter = "http://" + subconverSplit[0];
		}

		// suburl token 是映射前后 fakeUserID fakeHost
		let suburl = `https://${host}/convertersubrequest?`;
		if (url.search.length > 0) {
			suburl = `https://${host}/convertersubrequest${url.search}&`;
		}
		suburl += `token=${btoa(fakeUserID + "@" + fakeHost)}`;
		let ffetch = `${subconverter}/sub?target=${target}&url=${encodeURIComponent(suburl)}&insert=false\
&config=${encodeURIComponent(subConverterMode)}&udp=true&emoji=true&list=false&tfo=false&scv=true&fdn=false&sort=false&new_name=true`;

		try {
			let response = await fetchUrl(ffetch, 16000, null, userAgent);
			// 还原假信息为真
			// @ts-ignore
			return new Response((await response.text()).replace(new RegExp(fakeUserID, 'gm'), userID).replace(new RegExp(fakeHost, 'gm'), host), {
				// @ts-ignore
				status: response.status,
				// @ts-ignore
				statusText: response.statusText,
				// @ts-ignore
				headers: response.headers
			});
		} catch (err) {
			console.error('请求subconverter地址时出错: ' + ffetch, err);
			return new Response(`后端订阅转换服务错误`, {
				status: 500,
				headers: {
					'Content-Type': 'text/plain; charset=utf-8',
				}
			});
		}
	}

	if (isSubReq) {
		// 校验第三方后端订阅转换服务请求合法
		if (!url.searchParams.has('token') || !isBase64(url.searchParams.get('token'))) {
			return new Response(new Error("Illegal Requests").message, {
				status: 403,
				headers: {
					'Content-Type': 'text/html;charset=utf-8',
				}
			});
		}

		let token = atob(url.searchParams.get('token')).split('@');
		if (token.length !== 2) {
			return new Response(new Error("Illegal Requests").message, {
				status: 403,
				headers: {
					'Content-Type': 'text/html;charset=utf-8',
				}
			});
		}

		[fakeUserID, fakeHost] = token;
	}

	if (url.searchParams.get("DLSstr")) {
		DLSstr = url.searchParams.get("DLSstr");
	}

	let addresses = [];

	if (hasProxyParams) {
		// CF IP列表
		ADD = "";
		// CSV CF代理表格
		CSV = "";
		// CF优选生成器
		SUB = "";

		if (url.searchParams.get("cfproxylist")) {
			ADD = url.searchParams.get("cfproxylist").trim().split(/[,\s]+/).map(list => "api://" + list).join(',');
		}
		if (url.searchParams.get("cfproxycsv")) {
			CSV = url.searchParams.get("cfproxycsv");
		}
		if (url.searchParams.get("cfproxysub")) {
			SUB = url.searchParams.get("cfproxysub");
		}
	}

	if (ADD) {
		let res = await getReProxys(ADD, onlyTls);
		if (res.length > 0) {
			addresses = addresses.concat(res);
		}
	}
	if (CSV) {
		let res = await getReProxysFromCsv(CSV, onlyTls, DLSstr);
		if (res.length > 0) {
			addresses = addresses.concat(res);
		}
	}
	if (SUB) {
		let res = await getReProxysFromGener(SUB, fakeUserID, fakeHost, onlyTls);
		if (res.length > 0) {
			addresses = addresses.concat(res);
		}
	}

	let partTag = "";
	if (!isSubReq && (host.includes('.workers.dev') || host.includes('.pages.dev'))) {
		partTag += encodeURIComponent("--请绑定自定义域!");
	}

	// 这里query proxyip 会多一个api请求获取proxyip过程
	// let [proxyIP, proxyPort] = await processProxyip(url, PROXYIP, host, true);
	// &path=${encodeURIComponent("/?ed=2560&proxyip=" + proxyIP + ":" + proxyPort)}

	// 如果是isSubReq，需要设置替换为假信息， 根据 address:port 去重， tag相同+1递增
	let uniqueTags = new Map(Array.from(new Set(addresses.map(m => m[2]))).map(a => [a, 0]));

	let linkes = addresses.reduce((accMap, url_arr) => {
		// url_arr[0] ==> address
		// url_arr[1] ==> port
		// url_arr[2] ==> tagname
		// url_arr[3] ==> v_less 完整链接,可能为undefined, 当不为undefined时，要按需（!isSubReq）将fakeUserID、fakeHost 还原
		// 利用 uniqueAddr【address:port】去重
		let uniqueAddr = url_arr[0] + ":" + url_arr[1];
		let old = accMap.get(uniqueAddr);
		if (!(old && [...decodeURIComponent(old[0])].length >= [...decodeURIComponent(url_arr[2])].length)) {
			let tmpUserID = isSubReq ? fakeUserID : userID;
			let tmpHost = isSubReq ? fakeHost : host;
			// 没有 url_arr[3] 的配置默认链接
			let vess = url_arr[3] || `${atob(pt)}://${tmpUserID}${atob(at)}${url_arr[0]}:${url_arr[1]}?encryption=none\
&type=ws${onlyTls ? "&security=tls" : ""}&host=${tmpHost}&sni=${tmpHost}&path=${encodeURIComponent("/?ed=2560")}#${encodeURIComponent(url_arr[2])}`;

			if (!isSubReq && url_arr[3]) {
				vess = url_arr[3].replace(new RegExp(fakeUserID, 'gm'), userID).replace(new RegExp(fakeHost, 'gm'), host);
			}

			// 相同 tagname 递增
			let tag = "";
			let num = uniqueTags.get(url_arr[2]);
			uniqueTags.set(url_arr[2], ++num);
			if (num > 1) {
				tag = `%20${num}`;
			}
			vess += (tag + partTag);
			accMap.set(uniqueAddr, [url_arr[2], vess]);
		}
		return accMap;
	}, new Map()).values().toArray().map(m => m[1]).join('\n');

	return new Response(btoa(linkes), {
		status: 200,
		headers: { "Content-Type": "text/plain; charset=utf-8" },
	});
}

async function fetchConfig(config_str, needfetch = true, resolve = null, outTime = 8000) {
	// 避免 api:// 的链接调用循环
	let apiReference = new Set();
	let inner = async function (config_str) {
		return (await Promise.all(config_str.trim().split(/[\n,]+/).map(v => v.trim()).map(async str => {
			// 前面是# 号的是忽略的配置
			if (str.charAt(0) === '#') return;

			if (str.startsWith("api://")) {
				if (!needfetch) {
					return;
				}
				let furl = str.slice(6);
				try {
					let resp = await (await fetchUrl(furl, outTime, apiReference)).text();
					// 回调处理文件有 api://  的链接
					return inner(resp);
				} catch (err) {
					console.error('获取地址时出错: ' + str, err.message);
					return; // 如果有错误，直接返回
				}
			}
			// 不是 api:// 开头看是否需要进一步处理
			return typeof resolve === 'function' ? resolve(str) : str;
		}))).flat().filter(Boolean).map(ip => ip.trim());
	}

	return inner(config_str);
}

async function getReProxys(add, onlyTls) {
	if (!add || (add = add.trim()).length == 0) {
		return [];
	}
	let ips = await fetchConfig(add);

	return parseAddrLinks(ips, onlyTls);
}

async function getReProxysFromCsv(csv, onlyTls, DLSstr = 5) {
	if (!csv || (csv = csv.trim()).length == 0) {
		return [];
	}

	// csv 数据太多，默认是排序的，每个CSV表格只获取符合条件的前8条
	const [DLS = 5, MAXROW = 8] = String(DLSstr).split(":").map(d => isFinite(+d) ? +d : undefined);
	let addresses = [];

	const handleCSV = function (lines) {
		lines = lines.split('\n').map(txt => txt.trim()).filter(Boolean);
		if (!lines || lines.length === 0) {
			console.warn('CSV文件为空: ', csv);
			return;
		}
		let header = null;
		let huf = false;	// 标记header头是否重新更新
		let ipColIndex = -1;
		let portColIndex = -1;
		let tlsColIndex = -1;
		let countryColIndex = -1; // 国家
		let cityColIndex = -1;  // 城市
		let speedColIndex = -1; // csv 速度
		let idcColIndex = -1; // 数据中心
		let continentColIndex = -1; // 洲
		let speedUnits = ""; // csv 测速单位
		let maxrow = 0;

		for (let i = 0; i < lines.length; i++) {
			if (lines[i].length === 0) {
				continue;
			}
			if ((lines[i].includes('地址') || lines[i].toLowerCase().startsWith('ip'))
				&& (lines[i].includes('端口') || lines[i].toLowerCase().includes('port'))) {
				header = lines[i].toLowerCase().split(',').map(txt => txt.trim());
				huf = true;
			}
			if (!header) {
				console.warn('CSV文件缺少头部字段');
				return;
			}

			if (huf) {
				huf = false;
				maxrow = MAXROW;
				ipColIndex = header.findIndex(str => str.startsWith('ip') || str.includes('地址'));
				portColIndex = header.findIndex(str => str.startsWith('端口') || str.startsWith('port'));
				if (ipColIndex === -1) {
					console.warn('CSV文件缺少必需的字段');
					return;
				}

				tlsColIndex = header.indexOf('tls');
				countryColIndex = header.findIndex(str => str.includes('国家'));
				cityColIndex = header.findIndex(str => str.includes('城市') || str.includes('city'));
				speedColIndex = header.findLastIndex(item => item.includes("速度") || item.includes("speed"));
				idcColIndex = header.findLastIndex(item => item.includes("数据中心") || item.includes("idc"));
				continentColIndex = header.findLastIndex(item => item.includes("地区") || item.includes("region"));
				speedUnits = "";	// 换了header, 单位重新计算

				if (header[speedColIndex]?.includes('kb')) {
					speedUnits = "KB";
				}
				else if (header[speedColIndex]?.includes('mb')) {
					speedUnits = "MB";
				}

				continue;
			}

			if (MAXROW > 0 && maxrow < 1) {
				continue;
			}
			let columns = lines[i].split(',').map(txt => txt.trim());
			if (columns.length !== header.length) {
				console.warn('CSV文件数据错乱');
				return;
			}

			if (onlyTls && columns[tlsColIndex]?.toLowerCase() !== "true") continue;
			// 在数据中获取速度单位
			if (!speedUnits) {
				if (columns[speedColIndex]?.toLowerCase().includes('kb')) {
					speedUnits = "KB";
				} else if (columns[speedColIndex]?.toLowerCase().includes('mb')) {
					speedUnits = "MB";
				}
			}

			// 检查速度大于DLS(DLS 是MB)
			let dataSpeed = parseFloat(columns[speedColIndex]);
			if (DLS > 0 && !isNaN(dataSpeed)) {
				if (speedUnits === "KB") {
					dataSpeed = Math.round(dataSpeed / 10) / 100;
				}
				if (dataSpeed < DLS) {
					continue;
				}
			}

			// 端口
			let port = columns[portColIndex] || '443';
			if (!columns[portColIndex] && columns[tlsColIndex]?.toLowerCase() !== 'true') {
				port = '80';
			}

			let tag = "";
			if (columns[cityColIndex]) tag += "-" + columns[cityColIndex];
			if (columns[countryColIndex]) tag += "-" + columns[countryColIndex];
			if (columns[continentColIndex]) tag += "-" + columns[continentColIndex];
			if (columns[idcColIndex]) tag += "-" + columns[idcColIndex];

			if (tag.length == 0) tag += columns[ipColIndex];
			else tag = tag.slice(1);

			let address = [columns[ipColIndex], port, tag];
			addresses.push(address);
			MAXROW > 0 && maxrow--;
		}
	}

	let csvUrls = csv.split(/[,\s]+/);
	// 避免 api:// 的链接调用循环
	let apiReference = new Set();
	for (let furl of csvUrls) {
		try {
			let resp = await (await fetchUrl(furl, 12000, apiReference)).text();
			handleCSV(resp);
		} catch (err) {
			console.error('获取地址时出错: ' + furl, err.message);
			return []; // 如果有错误，直接返回
		}
	}

	return addresses;
}

async function getReProxysFromGener(generStr, fakeUserID, fakeHost, onlyTls = true) {
	if (!generStr || (generStr = generStr.trim()).length == 0) {
		return [];
	}

	let generNum = new Set();
	let fetch_sub = async function (sub) {
		// 订阅器地址
		let converSplit = sub.split("://");
		if (converSplit.length < 2) {
			sub = "https://" + converSplit[0];
		}
		// 一个Gener 生成的订阅地址够大，所以只允许3个
		if (generNum.has(sub) || generNum.size >= 3) {
			return;
		}
		generNum.add(sub);

		let url = `${sub}/sub?host=${fakeHost}&uuid=${fakeUserID}&path=${encodeURIComponent("/?ed=2560")}`;
		try {
			// 可能是base64编码串
			let encodeStr = await (await fetchUrl(url, 12000, null, 'v2ray.xray')).text();
			// 将Base64数据解码
			encodeStr = (isBase64(encodeStr) ? atob(encodeStr) : encodeStr);
			return encodeStr.split('\n');
		} catch (err) {
			console.error('解析ProxyGener地址时出错: ' + url, err);
			return; // 如果有错误，直接返回
		}
	}
	let ips = await fetchConfig(generStr, true, fetch_sub);

	return parseAddrLinks(ips, onlyTls, true);
}

function parseAddrLinks(ips, onlyTls, isVess = false) {
	// abc.com:端口#节点名
	// 123.123.123.123:端口#节点名
	// [abc:1234::1]:端口#节点名
	const urlReg = /^((?:https?:\/\/)?(?:[\w-]+\.)+[a-z-]+|\d{1,3}(?:\.\d{1,3}){3}|\[[a-f0-9:]+\])(?::(\d+))?(?:#(.+)$)?/i;
	const vessReg = new RegExp(`^${atob(pt)}://[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}${atob(at)}((?:[\\w-]+\\.)+[a-z-]+|\\d{1,3}(?:\\.\\d{1,3}){3}|\\[[a-f0-9:]+\\]):(\\d+)\\?[^#]+(?:#(.+)$)?`, "i");

	return ips.flatMap(ip => {
		let regExp = urlReg;
		if (isVess) {
			regExp = vessReg;
		}
		let match = regExp.exec(ip);
		if (!match) {
			return;
		}

		let [, address, port, tag = address] = match;
		if (isVess) {
			if (!match[3]) {
				ip += ((ip.slice(-1) === "#") ? "" : "#") + encodeURIComponent(tag);
			}
			return [[address, port, tag, ip]];
		}
		else if (!port) {
			// 没有设置端口的，根据 CF 默认几个端口返回
			let res = [];
			if (!onlyTls) {
				res = res.concat([[address, "80", tag]]);
				// res = res.concat(Array.from(HttpPort).map(port => {
				// 	return [address, port, tag]
				// }));
			}
			res = res.concat([[address, "443", tag]]);
			// res = res.concat(Array.from(HttpsPort).map(port => {
			// 	return [address, port, tag]
			// }));
			return res;
		}

		return [[address, port, tag]];
	}).filter(Boolean);
}

/**
 * Url fetch wrapper
 * @param {string} furl
 * @param {number} [outTime=0]
 * @param {Set} [apiAvoidDupRef=null]
 * @param {string} [UA="Mozilla/5.0 Chrome/131.0.0.0"]
 * @returns {Promise<Response>} fetch content
 */
function fetchUrl(furl, outTime = 0, apiAvoidDupRef = null, UA = "Mozilla/5.0 Chrome/131.0.0.0") {
	let converSplit = furl.split("://");
	if (converSplit.length < 2) {
		furl = "https://" + converSplit[0];
	}
	// 避免的链接调用循环
	if (apiAvoidDupRef) {
		if (apiAvoidDupRef.has(furl)) {
			throw new Error("URL duplicate request: " + furl);
		}
		apiAvoidDupRef.add(furl);
	}

	let abortc = null, id = null;
	if (typeof outTime === "number" && outTime > 0) {
		abortc = new AbortController();
		id = setTimeout(() => abortc.abort(), outTime);
	}
	const resp = fetch(furl, {
		method: 'get',
		headers: {
			'Accept': 'text/html,text/plain,application/xhtml+xml,text/yaml,application/json,application/x-yaml;',
			'User-Agent': UA
		},
		signal: abortc?.signal,
	}).finally(() => {
		id && clearTimeout(id);
	}).catch(error => {
		if (error.name === 'AbortError') {
			throw new Error(`请求超时 ${outTime}ms: ${furl}`);
		}
		throw error;
	});

	return resp;
}

function generateRandomUUID() {
	return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
		const r = Math.random() * 16 | 0;
		const v = c == "x" ? r : r & 3 | 8;
		return v.toString(16);
	});
}

function generateRandomStr(len) {
	return Math.random().toString(36).substring(2, len);
}

function isBase64(str) {
	if (!str || str.length % 4 !== 0) {
		return false;
	}
	try {
		return btoa(atob(str)) === str;
	} catch (err) {
		return false;
	}
}
