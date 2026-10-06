/* eslint-env worker */
/*
 * Hosts the minibwa Emscripten runtime off the page's main thread, so the user's input files
 * can be mounted by reference (WORKERFS reads a File synchronously, which only a worker may
 * do) instead of being copied into memory. This is a plain classic script: it is served as
 * is, not bundled, because it must importScripts the staged runtime under /wasm/.
 *
 * Requests are { requestId, type, ... } and are answered with { requestId, ok, result } or
 * { requestId, ok: false, error }. Program output is posted as { type: 'stdout' | 'stderr',
 * text } in batches, where each line is terminated by a newline.
 */
'use strict';

/**
 * A run is over once minibwa's final "Real time" line has been printed (it comes after every
 * output thread has finished) and output has then stayed quiet this long, so only messages
 * already in flight can still arrive.
 */
const QUIET_AFTER_DONE_MS = 500;
/**
 * minibwa prints no final line when it fails, so a run that has reported an error is over
 * once output has stopped this long.
 */
const QUIET_AFTER_ERROR_MS = 4500;
/**
 * Without either, all that is known is that output stopped. A large run is silent for as long
 * as a batch of reads (100 Mbp) takes to map, which in WASM is tens of seconds, so this is
 * only a last resort against waiting forever.
 */
const QUIET_AFTER_STALL_MS = 10 * 60_000;
const POLL_MS = 40;
/**
 * Output is posted when this much has gathered, or this long after the first line. The mapper
 * is never held back: the page copes with its pace by skipping what it cannot show, and it
 * can do that a batch at a time, so batches are kept small.
 */
const BATCH_BYTES = 64 * 1024;
const BATCH_MS = 50;

const buffers = { stdout: '', stderr: '' };
let batchTimer = null;
let lastOutputMs = Date.now();
let sawMainDone = false;
let sawError = false;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function flushOutput() {
	if (batchTimer !== null) {
		clearTimeout(batchTimer);
		batchTimer = null;
	}
	for (const type of ['stdout', 'stderr']) {
		if (buffers[type]) {
			self.postMessage({ type, text: buffers[type] });
			buffers[type] = '';
		}
	}
}

function append(type, text) {
	buffers[type] += text;
	if (buffers[type].length >= BATCH_BYTES) flushOutput();
	else if (batchTimer === null) batchTimer = setTimeout(flushOutput, BATCH_MS);
}

function emit(type, line) {
	lastOutputMs = Date.now();
	if (type === 'stderr') {
		if (line.includes('[M::main]') && line.includes('Real time:')) sawMainDone = true;
		else if (line.startsWith('ERROR') || line.startsWith('[E::')) sawError = true;
	}
	append(type, `${line}\n`);
}

/**
 * Emits `text`, which is whole lines (it ends with a newline), cut into batches at line
 * boundaries. One write by the mapper can hold megabytes; the page is fed it in small pieces,
 * as it always was, so that it can skip ahead between them.
 */
function emitLines(type, text) {
	lastOutputMs = Date.now();
	let start = 0;
	while (text.length - start > BATCH_BYTES) {
		const cut = text.indexOf('\n', start + BATCH_BYTES);
		if (cut < 0) break;
		append(type, text.slice(start, cut + 1));
		start = cut + 1;
	}
	if (start < text.length) append(type, start === 0 ? text : text.slice(start));
}

/*
 * The mapper's SAM output. Through stdout every byte goes one at a time through JavaScript
 * (Emscripten's terminal device), on the one thread that writes the output, which caps how
 * fast the mapper can run however many threads it has. A device of our own is handed each
 * write as one buffer instead. `minibwa map -o` opens it like a file.
 */
const OUTPUT_DEVICE = '/dev/minibwa-out';
const outputDecoder = new TextDecoder();
/** The unfinished last line of what has been written so far. */
let partialLine = '';
let hasOutputDevice = false;

function writeOutput(bytes) {
	const text = partialLine + outputDecoder.decode(bytes, { stream: true });
	const end = text.lastIndexOf('\n') + 1;
	partialLine = text.slice(end);
	if (end > 0) emitLines('stdout', end === text.length ? text : text.slice(0, end));
}

function closeOutput() {
	partialLine += outputDecoder.decode();
	if (partialLine) emit('stdout', partialLine);
	partialLine = '';
}

function registerOutputDevice(FS) {
	const device = FS.makedev(64, 0);
	FS.registerDevice(device, {
		open(stream) {
			stream.seekable = false;
		},
		close: closeOutput,
		// The buffer is the whole (shared) heap; a view of shared memory cannot be decoded,
		// so the bytes are copied out first.
		write(stream, buffer, offset, length) {
			if (length > 0) writeOutput(buffer.slice(offset, offset + length));
			return length;
		}
	});
	FS.mkdev(OUTPUT_DEVICE, device);
	hasOutputDevice = true;
}

let runtime = null;

function requireRuntime() {
	if (!runtime || !runtime.FS) throw new Error('minibwa WASM runtime is not ready.');
	return runtime;
}

function mkdirp(FS, path) {
	let current = '';
	for (const part of path.split('/').filter(Boolean)) {
		current += `/${part}`;
		try {
			FS.mkdir(current);
		} catch {
			// Already exists.
		}
	}
}

/** Waits until a pthread-run `main` has really finished; see the QUIET_AFTER constants. */
async function drainOutput() {
	for (;;) {
		const quietFor = Date.now() - lastOutputMs;
		if (sawMainDone && quietFor > QUIET_AFTER_DONE_MS) return;
		if (sawError && quietFor > QUIET_AFTER_ERROR_MS) return;
		if (quietFor > QUIET_AFTER_STALL_MS) return;
		await sleep(POLL_MS);
	}
}

const handlers = {
	init({ wasmBase }) {
		return new Promise((resolve, reject) => {
			self.Module = {
				noInitialRun: true,
				noExitRuntime: true,
				locateFile: (path) => `${wasmBase}${path}`,
				// Threads the runtime starts load this script, not the worker running it.
				mainScriptUrlOrBlob: `${wasmBase}minibwa.js`,
				print: (text) => emit('stdout', String(text)),
				printErr: (text) => emit('stderr', String(text)),
				onRuntimeInitialized: () => {
					runtime = self.Module;
					try {
						registerOutputDevice(runtime.FS);
					} catch (error) {
						// Output then goes through stdout, which is slower but works.
						emit('stderr', `Could not set up the bulk output device: ${error}`);
					}
					resolve({ crossOriginIsolated: self.crossOriginIsolated === true });
				}
			};
			try {
				importScripts(`${wasmBase}minibwa.js`);
			} catch (error) {
				reject(error);
			}
		});
	},

	mkdir({ path }) {
		mkdirp(requireRuntime().FS, path);
	},

	/** Mounts `files` (File objects) read-only at `dir`, by reference. */
	mount({ dir, files }) {
		const { FS } = requireRuntime();
		const WORKERFS = FS.filesystems && FS.filesystems.WORKERFS;
		if (!WORKERFS) {
			throw new Error(
				'This minibwa build has no WORKERFS. Rebuild it with "pixi run compile-wasm" and restage with "npm run stage:assets".'
			);
		}
		mkdirp(FS, dir);
		try {
			FS.unmount(dir);
		} catch {
			// Nothing was mounted there yet.
		}
		FS.mount(WORKERFS, { blobs: files.map(({ name, file }) => ({ name, data: file })) }, dir);
	},

	/**
	 * Runs a command. With `bulkOutput`, `args` is a `minibwa map` command and its output goes
	 * to the bulk output device (see OUTPUT_DEVICE) instead of stdout, if there is one.
	 */
	async run({ args, drain, bulkOutput }) {
		const { callMain } = requireRuntime();
		sawMainDone = false;
		sawError = false;
		lastOutputMs = Date.now();
		partialLine = '';
		const code = callMain(
			bulkOutput && hasOutputDevice ? [args[0], '-o', OUTPUT_DEVICE, ...args.slice(1)] : args
		);
		if (drain) await drainOutput();
		if (code && code !== 0) throw new Error(`minibwa exited with code ${code}`);
	}
};

self.onmessage = async ({ data }) => {
	const { requestId, type } = data;
	try {
		const handler = handlers[type];
		if (!handler) throw new Error(`Unknown request: ${type}`);
		const result = await handler(data);
		flushOutput();
		self.postMessage({ requestId, ok: true, result });
	} catch (error) {
		flushOutput();
		self.postMessage({
			requestId,
			ok: false,
			error: error && error.message ? error.message : String(error)
		});
	}
};
