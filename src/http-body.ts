/**
 * Deadline-aware response body reading, shared by the Telegram client and the
 * calendar fetcher.
 *
 * Contract:
 * - The deadline stays active after the headers arrive: every read races the
 *   signal and rejects with AbortError as soon as it fires, even when the
 *   underlying read never settles.
 * - Cancellation is fire-and-forget: no cleanup path awaits a stream's
 *   cancellation promise, which may never settle.
 * - readBoundedStream (used by readBoundedBody and the webhook intake) starts
 *   cancellation whenever it does not return a complete body - overflow,
 *   deadline and stream error - and releases the reader lock on every path.
 */

interface StreamReadResult {
  done: boolean;
  value?: Uint8Array;
}

/** Single failure style for every deadline path. */
function abortError(): DOMException {
  return new DOMException('aborted', 'AbortError');
}

/**
 * Reads one chunk, racing it against the deadline signal; the abort listener is
 * removed once the read settles either way.
 */
export function readWithAbort(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  deadline: AbortSignal,
): Promise<StreamReadResult> {
  if (deadline.aborted) {
    return Promise.reject(abortError());
  }
  return new Promise((resolve, reject) => {
    const onAbort = (): void => {
      reject(abortError());
    };
    deadline.addEventListener('abort', onAbort, { once: true });
    reader.read().then(
      (result) => {
        deadline.removeEventListener('abort', onAbort);
        resolve(result);
      },
      (error: unknown) => {
        deadline.removeEventListener('abort', onAbort);
        reject(error instanceof Error ? error : new Error(String(error)));
      },
    );
  });
}

/**
 * Starts cancellation without awaiting it: a stream is allowed to return a
 * cancellation promise that never settles, and cleanup must not be unbounded.
 */
export function cancelReader(reader: ReadableStreamDefaultReader<Uint8Array>): void {
  void reader.cancel().catch(() => undefined);
}

/** Cancels an unread body without awaiting an unbounded cleanup promise. */
export function cancelBody(response: Response): void {
  void response.body?.cancel().catch(() => undefined);
}

/**
 * Reads a byte stream up to `maxBytes` under the given deadline.
 *
 * Resolves with the decoded text (`''` for a null body), or null when the cap
 * is exceeded. Rejects with AbortError when the deadline fires, or with the
 * stream error. When the read does not finish, cancellation is started; the
 * reader lock is released on every path.
 */
export async function readBoundedStream(
  body: ReadableStream<Uint8Array> | null,
  maxBytes: number,
  deadline: AbortSignal,
): Promise<string | null> {
  if (body === null) {
    return '';
  }
  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let totalBytes = 0;
  try {
    for (;;) {
      const { done, value } = await readWithAbort(reader, deadline);
      if (done) {
        break;
      }
      if (value !== undefined) {
        totalBytes += value.byteLength;
        if (totalBytes > maxBytes) {
          cancelReader(reader);
          return null;
        }
        chunks.push(value);
      }
    }
  } catch (error) {
    cancelReader(reader);
    throw error;
  } finally {
    try {
      reader.releaseLock();
    } catch {
      // A pending cancellation owns the lock; the stream is already torn down.
    }
  }
  const merged = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(merged);
}

/** Bounded read of a fetched response body; the webhook path uses the stream form directly. */
export async function readBoundedBody(
  response: Response,
  maxBytes: number,
  deadline: AbortSignal,
): Promise<string | null> {
  return readBoundedStream(response.body, maxBytes, deadline);
}
