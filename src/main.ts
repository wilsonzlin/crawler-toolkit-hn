import {
  VArray,
  VBoolean,
  VInteger,
  VMember,
  VOptional,
  VString,
  VStruct,
} from "@wzlin/valid";
import assertExists from "@xtjs/lib/js/assertExists";
import asyncTimeout from "@xtjs/lib/js/asyncTimeout";
import mapExists from "@xtjs/lib/js/mapExists";
import raceAsyncIterables from "@xtjs/lib/js/raceAsyncIterables";
import { randomInt } from "node:crypto";

// Some items are literally null e.g. https://hacker-news.firebaseio.com/v0/item/0.json.
// Some items basically omit all properties e.g. https://hacker-news.firebaseio.com/v0/item/78692.json.
const vItem = new VOptional(
  new VStruct({
    id: new VInteger(),
    deleted: new VOptional(new VBoolean()),
    type: new VMember(["job", "story", "comment", "poll", "pollopt"] as const),
    by: new VOptional(new VString()),
    // This is represented as a UNIX timestamp in seconds since epoch.
    time: new VOptional(new VInteger()),
    // HTML.
    text: new VOptional(new VString()),
    dead: new VOptional(new VBoolean()),
    parent: new VOptional(new VInteger()),
    poll: new VOptional(new VInteger()),
    kids: new VOptional(new VArray(new VInteger())),
    url: new VOptional(new VString()),
    score: new VOptional(new VInteger()),
    // HTML.
    title: new VOptional(new VString()),
    parts: new VOptional(new VArray(new VInteger())),
    // Can be -1.
    descendants: new VOptional(new VInteger(-1)),
  }),
);

export type Post = {
  id: number;
  author: string | undefined;
  dead: boolean;
  deleted: boolean;
  score: number;
  textHtml: string;
  timestamp: Date | undefined;
  titleHtml: string;
  url: string | undefined;
};

export type Comment = {
  id: number;
  author: string | undefined;
  dead: boolean;
  deleted: boolean;
  parent: number;
  score: number;
  textHtml: string;
  timestamp: Date | undefined;
};

type EarlyStopState = {
  // Smallest ID that we stopped at. Consider that if one worker stopped at `x` and another at `x + 3`, we must set `nextId` to `x` and not `x + 3`, as otherwise we'll skip `x` (even though we have `x + 1` and `x + 2`).
  id: number | undefined;
  // If we prematurely stopped, we know exactly how long to wait until the next earliest available item.
  ts: Date | undefined;
};

export const fetchHnMaxId = async () => {
  const res = await fetch("https://hacker-news.firebaseio.com/v0/maxitem.json");
  const raw = await res.json();
  return new VInteger(0).parseRoot(raw);
};

export class FetchHnBadStatusError extends Error {
  constructor(
    readonly status: number,
    readonly body: string,
  ) {
    super(`Fetching HN item failed with status ${status}: ${body}`);
  }
}

export const fetchHnItem = async (
  id: number,
  {
    timeoutMs = 1000 * 12,
    onRetry,
  }: {
    timeoutMs?: number;
    onRetry?: (err: Error, attempt: number) => void;
  },
): Promise<{
  post?: Post;
  comment?: Comment;
}> => {
  let raw;
  for (let attempt = 1; ; attempt++) {
    const ctl = new AbortController();
    setTimeout(() => ctl.abort(), timeoutMs);
    try {
      const res = await fetch(
        `https://hacker-news.firebaseio.com/v0/item/${id}.json`,
        {
          signal: ctl.signal,
        },
      );
      const rawText = await res.text();
      if (!res.ok) {
        throw new FetchHnBadStatusError(res.status, rawText);
      }
      raw = JSON.parse(rawText);
      break;
    } catch (error) {
      if (error instanceof FetchHnBadStatusError && error.status < 500) {
        throw error;
      }
      onRetry?.(error, attempt);
      await asyncTimeout(randomInt(1000 * (1 << attempt)));
    }
  }
  const item = vItem.parseRoot(raw);
  if (item?.type === "story") {
    const post: Post = {
      id,
      author: item.by,
      dead: item.dead ?? false,
      deleted: item.deleted ?? false,
      score: item.score ?? 0,
      textHtml: item.text ?? "",
      timestamp: mapExists(item.time, (ts) => new Date(ts * 1000)),
      titleHtml: item.title ?? "",
      url: item.url,
    };
    return { post };
  } else if (item?.type === "comment") {
    const comment: Comment = {
      id,
      author: item.by,
      dead: item.dead ?? false,
      deleted: item.deleted ?? false,
      parent: assertExists(item.parent),
      score: item.score ?? 0,
      textHtml: item.text ?? "",
      timestamp: mapExists(item.time, (ts) => new Date(ts * 1000)),
    };
    return { comment };
  } else {
    return {};
  }
};

async function* innerWorker({
  earlyStopState,
  fetchItemTimeoutMs,
  ids,
  onItemFetchRetry,
  stopOnItemWithinDurationMs,
}: {
  earlyStopState: EarlyStopState;
  fetchItemTimeoutMs?: number;
  ids: number[];
  onItemFetchRetry?: (err: Error, attempt: number) => void;
  stopOnItemWithinDurationMs?: number;
}) {
  while (true) {
    const id = ids.shift();
    // `ids` is a queue where entries are ascending, so if a worker has already stopped at an earlier ID, we can just stop entirely.
    if (
      id == undefined ||
      (earlyStopState.id != undefined && id > earlyStopState.id)
    ) {
      break;
    }
    const { post, comment } = await fetchHnItem(id, {
      onRetry: onItemFetchRetry,
      timeoutMs: fetchItemTimeoutMs,
    });
    const ts = post?.timestamp ?? comment?.timestamp;
    if (
      ts &&
      stopOnItemWithinDurationMs != undefined &&
      Date.now() - ts.getTime() <= stopOnItemWithinDurationMs
    ) {
      if (earlyStopState.id == undefined || id < earlyStopState.id) {
        earlyStopState.id = id;
      }
      if (
        earlyStopState.ts == undefined ||
        ts.getTime() < earlyStopState.ts.getTime()
      ) {
        earlyStopState.ts = ts;
      }
      // Do not submit.
      continue;
    }
    yield { id, post, comment };
  }
}

/**
 * This will crawl HN items (posts and comments) from {@param nextId} until:
 * - the item with ID {@param maxId} if provided; or
 * - the item that has a timestamp within {@param stopOnItemWithinDurationMs} of now, if provided; or
 * - the max ID provided by the HN API.
 *
 * This will spawn {@param concurrency} background Promises, each of which will continuously make exactly one HTTP request at any one time to fetch items.
 *
 * This async generator will always yield items in order of ID ascending, even if the background Promises fetch things out of order. This makes it easy to correctly persist the `nextId` state in some persistence store for resuming later; if items were not yielded in order, the state could clobber each other. Some items are neither posts nor comments; the `nextId` state should still be persisted.
 */
export async function* crawlHn({
  concurrency = 64,
  fetchItemTimeoutMs,
  maxId: forceMaxId,
  nextId,
  onItemFetchRetry,
  stopOnItemWithinDurationMs,
}: {
  concurrency?: number;
  maxId?: number;
  nextId: number;
  // It may be worth stopping once a comment or post that was created within this duration from now is reached, as subsequent items may be changed (votes, contents, flagging) a lot and not near a "final state".
  stopOnItemWithinDurationMs?: number;
  onItemFetchRetry?: (err: Error, attempt: number) => void;
  fetchItemTimeoutMs?: number;
}) {
  // We must submit nulls (i.e. IDs of items with no value) and cannot simply use a separate counter as we only know if it's null after a fetch and that's asynchronous and IDs could get reordered. (The point of this is to ensure that the next ID state in database is persisted without skipping.)
  const completed = new Map<
    number,
    {
      post?: Post;
      comment?: Comment;
    }
  >();
  let nextIdToYield = nextId;

  const maxId = forceMaxId ?? (await fetchHnMaxId());
  const ids = Array.from({ length: maxId - nextId + 1 }, (_, i) => nextId + i);
  const earlyStopState: EarlyStopState = {
    id: undefined,
    ts: undefined,
  };
  for await (const { id, post, comment } of raceAsyncIterables(
    ...Array.from({ length: concurrency }, () =>
      innerWorker({
        earlyStopState,
        fetchItemTimeoutMs,
        ids,
        onItemFetchRetry,
        stopOnItemWithinDurationMs,
      }),
    ),
  )) {
    completed.set(id, { post, comment });
    while (completed.has(nextIdToYield)) {
      let c;
      while ((c = completed.get(nextIdToYield))) {
        completed.delete(nextIdToYield);
        yield {
          // This must always be provided, because both `post` and `comment` could be undefined.
          newNextId: ++nextIdToYield,
          post: c.post,
          comment: c.comment,
        };
      }
    }
  }
  return earlyStopState;
}
