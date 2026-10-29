import { FrameDecoder, encodeFrame } from "./framing";
import { receiveControl, sendControl } from "./relay";
import type { CrocSocket } from "./transport";
import type { CrocMessage } from "./types";

// WebRTC gives the browser its only direct path to a peer. When both sides
// advertise WEBRTC_FEATURE, the sender ends negotiation with a
// transport-select message: "webrtc" after an offer/answer exchange on the
// encrypted control channel, or "relay". Only STUN is configured, so a
// failed connection falls back to the croc relay rather than to a TURN relay.
export const WEBRTC_FEATURE = "webrtc-v1";
export const TRANSPORT_WEBRTC = "webrtc";
export const TRANSPORT_RELAY = "relay";
export const DEFAULT_STUN_SERVERS = [
  "stun:stun.l.google.com:19302",
  "stun:stun1.l.google.com:19302",
];
const CHANNELS = 4;
const MAX_CHANNELS = 16;
const SETUP_TIMEOUT_MS = 12_000;
// Every browser's SCTP stack accepts 16 KiB messages.
const MAX_MESSAGE_SIZE = 16 * 1024;
const HIGH_WATER = 4 * 1024 * 1024;
const LOW_WATER = 1024 * 1024;
const LABEL_PREFIX = "croc-";

// DataSocket is what file transfers need from a data stream; CrocSocket and
// DataChannelSocket both provide it.
export interface DataSocket {
  send(payload: Uint8Array): Promise<void>;
  receive(skipPings?: boolean): Promise<Uint8Array>;
  close(): void;
}

export function webrtcSupported() {
  return typeof RTCPeerConnection !== "undefined";
}

type Reader = {
  resolve(value: Uint8Array): void;
  reject(reason: Error): void;
};

type DataChannelLike = Pick<
  RTCDataChannel,
  | "binaryType"
  | "bufferedAmount"
  | "bufferedAmountLowThreshold"
  | "readyState"
  | "send"
  | "close"
  | "addEventListener"
  | "removeEventListener"
>;

// DataChannelSocket carries croc's framed messages over a data channel as a
// byte stream, split into MAX_MESSAGE_SIZE messages, with bufferedAmount
// backpressure.
export class DataChannelSocket implements DataSocket {
  private decoder = new FrameDecoder();
  private messages: Uint8Array[] = [];
  private readers: Reader[] = [];
  private failure?: Error;
  private closed = false;

  constructor(
    private channel: DataChannelLike,
    private onClose: () => void = () => {},
    signal?: AbortSignal,
  ) {
    if (signal?.aborted) {
      queueMicrotask(() => this.close());
    } else {
      signal?.addEventListener("abort", () => this.close(), { once: true });
    }
    channel.binaryType = "arraybuffer";
    channel.bufferedAmountLowThreshold = LOW_WATER;
    channel.addEventListener("message", (event: Event) => {
      const data = (event as MessageEvent).data;
      if (!(data instanceof ArrayBuffer)) {
        this.fail(new Error("WebRTC data channel sent a non-binary message"));
        return;
      }
      try {
        for (const message of this.decoder.push(new Uint8Array(data))) {
          this.deliver(message);
        }
      } catch (error) {
        this.fail(error instanceof Error ? error : new Error(String(error)));
        this.close();
      }
    });
    channel.addEventListener("close", () => {
      this.fail(new Error("WebRTC data channel closed"));
    });
  }

  async send(payload: Uint8Array) {
    const frame = encodeFrame(payload);
    for (let offset = 0; offset < frame.byteLength; offset += MAX_MESSAGE_SIZE) {
      if (this.failure) throw this.failure;
      if (this.channel.readyState !== "open") {
        throw new Error("WebRTC data channel is not open");
      }
      if (this.channel.bufferedAmount > HIGH_WATER) await this.drained();
      this.channel.send(
        frame.slice(offset, Math.min(offset + MAX_MESSAGE_SIZE, frame.byteLength)),
      );
    }
  }

  private drained() {
    return new Promise<void>((resolve, reject) => {
      const done = () => {
        this.channel.removeEventListener("bufferedamountlow", done);
        this.channel.removeEventListener("close", closed);
        resolve();
      };
      const closed = () => {
        this.channel.removeEventListener("bufferedamountlow", done);
        this.channel.removeEventListener("close", closed);
        reject(this.failure ?? new Error("WebRTC data channel closed"));
      };
      this.channel.addEventListener("bufferedamountlow", done);
      this.channel.addEventListener("close", closed);
    });
  }

  async receive(skipPings = true): Promise<Uint8Array> {
    for (;;) {
      const message = await this.next();
      if (skipPings && message.byteLength === 1 && message[0] === 1) continue;
      return message;
    }
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    if (this.channel.readyState === "open" || this.channel.readyState === "connecting") {
      this.channel.close();
    }
    this.fail(new Error("WebRTC data channel closed"));
    this.onClose();
  }

  private next() {
    if (this.messages.length > 0) return Promise.resolve(this.messages.shift()!);
    if (this.failure) return Promise.reject(this.failure);
    return new Promise<Uint8Array>((resolve, reject) => {
      this.readers.push({ resolve, reject });
    });
  }

  private deliver(message: Uint8Array) {
    const reader = this.readers.shift();
    if (reader) reader.resolve(message);
    else this.messages.push(message);
  }

  private fail(error: Error) {
    if (this.failure) return;
    this.failure = error;
    for (const reader of this.readers.splice(0)) reader.reject(error);
  }
}

class SetupTimeoutError extends Error {}

function withTimeout<T>(promise: Promise<T>, ms: number, what: string, signal?: AbortSignal) {
  return new Promise<T>((resolve, reject) => {
    const timer = window.setTimeout(() => {
      cleanup();
      reject(new SetupTimeoutError(`Timed out waiting for ${what}`));
    }, ms);
    const onAbort = () => {
      cleanup();
      reject(new DOMException("Transfer cancelled", "AbortError"));
    };
    const cleanup = () => {
      window.clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => {
        cleanup();
        resolve(value);
      },
      (error) => {
        cleanup();
        reject(error);
      },
    );
  });
}

function iceGathered(pc: RTCPeerConnection) {
  if (pc.iceGatheringState === "complete") return Promise.resolve();
  return new Promise<void>((resolve) => {
    const check = () => {
      if (pc.iceGatheringState !== "complete") return;
      pc.removeEventListener("icegatheringstatechange", check);
      resolve();
    };
    pc.addEventListener("icegatheringstatechange", check);
  });
}

function channelOpen(channel: RTCDataChannel) {
  if (channel.readyState === "open") return Promise.resolve();
  return new Promise<void>((resolve, reject) => {
    channel.addEventListener("open", () => resolve(), { once: true });
    channel.addEventListener(
      "close",
      () => reject(new Error("WebRTC data channel closed before opening")),
      { once: true },
    );
  });
}

function channelIndex(label: string) {
  if (!label.startsWith(LABEL_PREFIX)) return -1;
  const index = Number(label.slice(LABEL_PREFIX.length));
  return Number.isInteger(index) && index >= 0 && index < MAX_CHANNELS ? index : -1;
}

// socketsFor wraps open channels, closing the peer connection once every
// socket has been closed. Cancelling the transfer closes every socket, like
// the relay's data sockets.
function socketsFor(pc: RTCPeerConnection, channels: RTCDataChannel[], signal?: AbortSignal) {
  let open = channels.length;
  const closed = () => {
    open -= 1;
    if (open === 0) pc.close();
  };
  return channels.map((channel) => new DataChannelSocket(channel, closed, signal));
}

// closeOnAbort closes a peer connection that has no sockets yet when the
// transfer is cancelled.
function closeOnAbort(pc: RTCPeerConnection, signal?: AbortSignal) {
  const close = () => pc.close();
  signal?.addEventListener("abort", close, { once: true });
  return () => signal?.removeEventListener("abort", close);
}

function newPeerConnection(stunServers: string[]) {
  return new RTCPeerConnection({
    iceServers: stunServers.length > 0 ? [{ urls: stunServers }] : [],
  });
}

// offerWebRTC runs the sender's half of the exchange. It returns open data
// sockets, or undefined when no direct path appeared; the caller then selects
// the relay.
export async function offerWebRTC(
  control: CrocSocket,
  key: Uint8Array,
  stunServers: string[],
  signal?: AbortSignal,
): Promise<DataSocket[] | undefined> {
  const started = Date.now();
  const remaining = () => Math.max(SETUP_TIMEOUT_MS - (Date.now() - started), 0);
  let pc: RTCPeerConnection | undefined;
  let releaseAbort = () => {};
  let channels: RTCDataChannel[];
  let opened: Promise<void[]>;
  try {
    const connection = newPeerConnection(stunServers);
    pc = connection;
    releaseAbort = closeOnAbort(connection, signal);
    channels = Array.from({ length: CHANNELS }, (_, index) =>
      connection.createDataChannel(`${LABEL_PREFIX}${index}`, { ordered: true }),
    );
    opened = Promise.all(channels.map(channelOpen));
    opened.catch(() => {});
    await connection.setLocalDescription(await connection.createOffer());
    await withTimeout(iceGathered(connection), remaining(), "ICE candidates", signal);
  } catch (error) {
    pc?.close();
    if (error instanceof DOMException && error.name === "AbortError") throw error;
    // No offer was sent, so the receiver is still waiting for a selection.
    return undefined;
  }
  await sendControl(
    control,
    { t: "webrtc-offer", m: pc.localDescription?.sdp ?? "", n: CHANNELS },
    key,
  );
  let answer: CrocMessage;
  try {
    // On a timeout this read stays first in the control socket's queue, so it
    // consumes the late answer and the next read gets the receiver's
    // following message.
    answer = await withTimeout(
      receiveControl(control, key),
      remaining(),
      "the WebRTC answer",
      signal,
    );
  } catch (error) {
    pc.close();
    if (error instanceof SetupTimeoutError) return undefined;
    throw error;
  }
  if (answer.t === "error") {
    pc.close();
    throw new Error(answer.m || "Peer cancelled the transfer");
  }
  if (answer.t !== "webrtc-answer") {
    pc.close();
    throw new Error(`Expected a WebRTC answer, got ${answer.t}`);
  }
  if (!answer.m) {
    pc.close();
    return undefined;
  }
  try {
    await pc.setRemoteDescription({ type: "answer", sdp: answer.m });
    await withTimeout(opened, remaining(), "WebRTC data channels", signal);
  } catch (error) {
    pc.close();
    if (error instanceof DOMException && error.name === "AbortError") throw error;
    return undefined;
  }
  releaseAbort();
  return socketsFor(pc, channels, signal);
}

export interface WebRTCAnswer {
  // wait resolves the open data sockets once the sender selects WebRTC.
  wait(signal?: AbortSignal): Promise<DataSocket[]>;
  close(): void;
}

// answerWebRTC answers the sender's offer. An answer it cannot create is sent
// empty, so the sender selects the relay instead of waiting.
export async function answerWebRTC(
  control: CrocSocket,
  key: Uint8Array,
  offer: CrocMessage,
  stunServers: string[],
  signal?: AbortSignal,
): Promise<WebRTCAnswer | undefined> {
  const count = offer.n ?? 0;
  let pc: RTCPeerConnection | undefined;
  let sdp = "";
  const channels: RTCDataChannel[] = [];
  let opened: Promise<void[]> | undefined;
  let gotAll: () => void = () => {};
  const allArrived = new Promise<void>((resolve) => {
    gotAll = resolve;
  });
  try {
    if (!Number.isInteger(count) || count < 1 || count > MAX_CHANNELS || !offer.m) {
      throw new Error("Invalid WebRTC offer");
    }
    pc = newPeerConnection(stunServers);
    closeOnAbort(pc, signal);
    pc.addEventListener("datachannel", (event) => {
      const index = channelIndex(event.channel.label);
      if (index < 0 || index >= count || channels[index]) {
        event.channel.close();
        return;
      }
      channels[index] = event.channel;
      if (channels.filter(Boolean).length === count) gotAll();
    });
    opened = allArrived.then(() => Promise.all(channels.map(channelOpen)));
    opened.catch(() => {});
    await pc.setRemoteDescription({ type: "offer", sdp: offer.m });
    await pc.setLocalDescription(await pc.createAnswer());
    await withTimeout(iceGathered(pc), SETUP_TIMEOUT_MS, "ICE candidates", signal);
    sdp = pc.localDescription?.sdp ?? "";
  } catch (error) {
    pc?.close();
    pc = undefined;
    if (error instanceof DOMException && error.name === "AbortError") throw error;
  }
  await sendControl(control, { t: "webrtc-answer", m: sdp }, key);
  if (!pc || !opened) return undefined;
  const connection = pc;
  const channelsOpened = opened;
  return {
    async wait(waitSignal = signal) {
      await withTimeout(channelsOpened, SETUP_TIMEOUT_MS, "WebRTC data channels", waitSignal);
      return socketsFor(connection, channels, waitSignal);
    },
    close() {
      connection.close();
    },
  };
}
